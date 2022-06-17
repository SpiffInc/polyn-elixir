defmodule Polyn.Serializers.JSON do
  @moduledoc """
  JSON Serializer for Polyn events. Functions will raise if
  inconsistencies are found
  """

  alias Polyn.Event
  alias Polyn.SchemaStore

  @doc """
  Convert a JSON payload into a Polyn.Event struct
  Raises an error if json is not valid
  """
  @spec deserialize(json :: binary(), conn :: Gnat.t()) ::
          {:ok, Polyn.Event.t()} | {:error, binary()}
  def deserialize(json, conn, opts \\ []) do
    with {:ok, data} <- decode(json),
         {:ok, json} <- validate(data, conn, opts) do
      {:ok, to_event(json)}
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
  @spec serialize!(event :: Polyn.Event.t(), conn :: Gnat.t()) :: String.t()
  def serialize!(%Event{} = event, conn, opts \\ []) do
    Map.from_struct(event)
    |> Enum.reduce(%{}, fn field, acc ->
      serialize_field(acc, field)
    end)
    |> add_datacontenttype()
    |> validate!(conn, opts)
    |> Jason.encode!()
  end

  defp serialize_field(data, {key, value}) do
    Map.put(data, Atom.to_string(key), value)
  end

  defp add_datacontenttype(%{"datacontenttype" => nil} = json) do
    Map.put(json, "datacontenttype", "application/json")
  end

  defp add_datacontenttype(json), do: json

  defp validate!(json, conn, opts) do
    case validate(json, conn, opts) do
      {:ok, json} ->
        json

      {:error, message} ->
        raise Polyn.ValidationException, message
    end
  end

  defp validate(json, conn, opts) do
    with {:ok, type} <- get_event_type(json),
         {:ok, schema} <- get_schema(conn, type, opts),
         :ok <- validate_schema(schema, json) do
      {:ok, json}
    else
      {:error, errors} ->
        {:error, handle_errors(errors, json)}
    end
  end

  defp get_schema(conn, type, opts) do
    case SchemaStore.get(conn, type, name: store_name(opts)) do
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

  defp validate_schema(schema, json) do
    case ExJsonSchema.Validator.validate(schema, json) do
      :ok ->
        :ok

      {:error, json_errors} ->
        {:error,
         Enum.map(json_errors, fn {message, property_path} ->
           "Property: `#{property_path}` - #{message}"
         end)}
    end
  end

  defp handle_errors(errors, json) do
    errors = add_error(errors, "Polyn event #{json["id"]} from #{json["source"]} is not valid")
    errors = errors ++ ["Event data: #{inspect(json)}"]
    Enum.join(errors, "\n")
  end

  defp add_error(errors, error) do
    [error | errors]
  end

  defp store_name(opts) do
    Keyword.get(opts, :store_name, SchemaStore.store_name())
  end
end
