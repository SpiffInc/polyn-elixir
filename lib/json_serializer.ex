defmodule Polyn.Serializers.JSON do
  # JSON Serializer for Polyn events. Functions will raise if
  # inconsistencies are found
  @moduledoc false

  alias Polyn.Naming

  @user_schemas_dir "/priv/polyn/schemas"

  @doc """
  Convert a JSON payload into a Polyn.Event struct
  Raises an error if json is not valid
  """
  @spec deserialize(json :: binary()) :: Polyn.Event.t()
  def deserialize(json) do
    Jason.decode!(json) |> validate() |> to_event()
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
  @spec serialize(event :: Polyn.Event.t()) :: String.t()
  def serialize(event) do
    Map.from_struct(event)
    |> stringify_keys()
    |> add_datacontenttype()
    |> validate()
    |> Jason.encode!()
  end

  # The validator library expects all map keys to be strings
  defp stringify_keys(event) do
    Jason.encode!(event) |> Jason.decode!()
  end

  defp add_datacontenttype(%{"datacontenttype" => nil} = json) do
    Map.put(json, "datacontenttype", "application/json")
  end

  defp add_datacontenttype(json), do: json

  defp validate(json) do
    validate_event_schema([], json)
    |> validate_dataschema(json)
    |> handle_errors(json)
  end

  defp validate_event_schema(errors, json) do
    case get_cloud_event_schema(json["specversion"]) do
      {:error, _message} ->
        add_error(errors, "Polyn does not recognize specversion #{json["specversion"]}")

      schema ->
        validate_schema(errors, schema, json)
        |> IO.inspect(label: "event spec errors")
    end
  end

  defp get_cloud_event_schema(version) do
    Polyn.CloudEvent.json_schema_for_version(version)
  end

  defp validate_dataschema(errors, json) when is_map_key(json, "dataschema") == false do
    validate_dataschema(errors, Map.put(json, "dataschema", nil))
  end

  defp validate_dataschema(errors, %{"dataschema" => nil}) do
    add_error(errors, "Missing dataschema. Every Polyn event must have a dataschema")
  end

  defp validate_dataschema(errors, json) do
    case get_dataschema(json["type"], json["dataschema"]) do
      {:ok, schema} ->
        validate_schema(errors, Jason.decode!(schema), json["data"])
        |> IO.inspect(label: "data spec errors")

      {:error, _reason} ->
        add_error(errors, "Polyn could not find dataschema #{json["dataschema"]}")
    end
  end

  defp get_dataschema(event_type, dataschema) do
    event_type = Naming.trim_domain_prefix(event_type)
    dataschema = Naming.colon_to_dot(dataschema <> ".json") |> Naming.trim_domain_prefix()

    file().read(Path.join(dataschema_dir(event_type), dataschema))
  end

  defp dataschema_dir("polyn" <> _suffix = event_type) do
    Application.app_dir(:polyn, ["priv", "migration_events", event_type])
  end

  defp dataschema_dir(event_type) do
    Path.join([file().cwd!(), @user_schemas_dir, event_type])
  end

  defp validate_schema(errors, schema, json) do
    schema_root = ExJsonSchema.Schema.resolve(schema)

    case ExJsonSchema.Validator.validate(schema_root, json) do
      :ok ->
        errors

      {:error, json_errors} ->
        Enum.map(json_errors, fn {message, property_path} ->
          "Property: `#{property_path}` - #{message}"
        end)
        |> add_error("JSON Schema with id #{schema["$id"]} did not pass validation")
        |> Enum.concat(errors)
    end
  end

  defp handle_errors([], json), do: json

  defp handle_errors(errors, json) do
    errors = add_error(errors, "Polyn event #{json["id"]} is not valid")
    errors = errors ++ ["Event data: #{inspect(json)}"]
    errors = Enum.join(errors, "\n")
    raise Polyn.ValidationException, errors
  end

  defp add_error(errors, error) do
    [error | errors]
  end

  defp file do
    Application.get_env(:polyn, :file, File)
  end
end
