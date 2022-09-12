defmodule Polyn.Serializers.JSON do
  # JSON Serializer for Polyn events. Functions will raise if
  # inconsistencies are found
  @moduledoc false

  alias Polyn.Event
  alias Polyn.SchemaStore

  @doc """
  Convert a JSON payload into a Polyn.Event struct
  """
  @spec deserialize(json :: binary()) ::
          {:ok, Polyn.Event.t()} | {:error, binary()}
  def deserialize(json, opts \\ []) do
    with {:ok, data} <- decode(json),
         {:ok, json} <- validate(data, opts) do
      {:ok, to_event(json)}
    end
  end

  @spec deserialize!(json :: binary()) :: Polyn.Event.t()
  def deserialize!(json, opts \\ []) do
    case deserialize(json, opts) do
      {:ok, event} -> event
      {:error, error} -> raise Polyn.ValidationException, error
    end
  end

  defp decode(json) do
    case Jason.decode(json) do
      {:error, error} ->
        {:error,
         "Polyn was unable to decode the following message: \n" <>
           "#{error.data} \n There were errors at position #{error.position}. \n " <>
           "Please ensure your message structure conforms to the CloudEvent schema and that your " <>
           "message data follows a JSON Schema registered using Polyn CLI."}

      success ->
        success
    end
  end

  defp to_event(json) do
    Map.keys(%Polyn.Event{})
    |> Enum.reduce(Keyword.new(), fn event_key, acc ->
      string_key = Atom.to_string(event_key)

      if Map.has_key?(json, string_key) do
        Keyword.put(acc, event_key, json[string_key])
      else
        acc
      end
    end)
    |> Polyn.Event.new()
  end

  @doc """
  Convert a Polyn.Event struct into a JSON paylod.
  Raises an error if event is not valid
  """
  @spec serialize!(event :: Polyn.Event.t()) :: String.t()
  def serialize!(%Event{} = event, opts \\ []) do
    Map.from_struct(event)
    |> add_datacontenttype()
    |> atom_keys_to_strings()
    |> validate!(opts)
    |> Jason.encode!()
  end

  defp add_datacontenttype(%{datacontenttype: nil} = json) do
    Map.put(json, :datacontenttype, "application/json")
  end

  defp add_datacontenttype(json), do: json

  # The validator lib requires that all map keys be strings
  defp atom_keys_to_strings(data) do
    Jason.encode!(data)
    |> Jason.decode!()
  end

  defp validate!(json, opts) do
    case validate(json, opts) do
      {:ok, json} ->
        json

      {:error, message} ->
        raise Polyn.ValidationException, message
    end
  end

  defp validate(json, opts) do
    with :ok <- validate_cloud_event(json),
         {:ok, type} <- get_event_type(json),
         :ok <- validate_event_type(type),
         {:ok, schema} <- get_schema(type, opts),
         :ok <- validate_schema(schema, json) do
      {:ok, json}
    else
      {:error, errors} ->
        {:error, handle_errors(errors, json)}
    end
  end

  # We want to make sure the json looks like a CloudEvent
  # and isn't some other datatype that can't even be parsed.
  # This is important for protecting against times when services use
  # a vanilla Gnat.pub or isn't publishing events through Polyn for
  # some other reason
  defp validate_cloud_event(json) do
    schema =
      Application.app_dir(:polyn, "priv/polyn/cloud_event_schema.json")
      |> File.read!()
      |> Jason.decode!()
      |> ExJsonSchema.Schema.resolve()

    case ExJsonSchema.Validator.validate(schema, json) do
      :ok ->
        :ok

      {:error, json_errors} ->
        {:error, format_schema_validation_errors(json_errors)}
    end
  end

  defp get_schema(type, opts) do
    case SchemaStore.get(store_name(opts), type) do
      nil ->
        {:error,
         [
           "Schema for #{type} does not exist. Make sure it's " <>
             "been added to your `events` codebase and has been loaded into the schema store on your NATS " <>
             "server"
         ]}

      schema ->
        {:ok, ExJsonSchema.Schema.resolve(schema)}
    end
  end

  defp get_event_type(json) do
    case json["type"] do
      nil ->
        {:error,
         [
           "Could not find a `type` in message #{inspect(json)} \n" <>
             "Every event must have a `type`"
         ]}

      type ->
        {:ok, Polyn.Naming.trim_domain_prefix(type)}
    end
  end

  defp validate_event_type(type) do
    case Polyn.Naming.validate_event_type(type) do
      {:error, reason} -> {:error, [reason]}
      success -> success
    end
  end

  defp validate_schema(schema, json) do
    case ExJsonSchema.Validator.validate(schema, json) do
      :ok ->
        :ok

      {:error, json_errors} ->
        {:error, format_schema_validation_errors(json_errors)}
    end
  end

  defp format_schema_validation_errors(json_errors) do
    Enum.map(json_errors, fn {message, property_path} ->
      "Property: `#{property_path}` - #{message}"
    end)
  end

  defp handle_errors(errors, json) when is_map(json) do
    errors = add_error(errors, "Polyn event #{json["id"]} from #{json["source"]} is not valid")
    errors = errors ++ ["Event data: #{inspect(json)}"]
    Enum.join(errors, "\n")
  end

  defp handle_errors(errors, json) do
    errors = add_error(errors, "Polyn events need to follow the CloudEvent spec")
    errors = errors ++ ["Message received: #{inspect(json)}"]
    Enum.join(errors, "\n")
  end

  defp add_error(errors, error) do
    [error | errors]
  end

  defp store_name(opts) do
    Keyword.get(opts, :store_name) |> SchemaStore.process_name()
  end
end
