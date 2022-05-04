defmodule Polyn.Serializers.JSON do
  @moduledoc """
  JSON Serializer for Polyn events. Functions will raise if
  inconsistencies are found
  """

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
    |> Enum.reduce(%{}, fn field, acc ->
      serialize_field(acc, field)
    end)
    |> add_datacontenttype()
    |> validate()
    |> Jason.encode!()
  end

  defp serialize_field(data, {key, value}) do
    Map.put(data, Atom.to_string(key), value)
  end

  defp add_datacontenttype(%{"data" => nil} = json), do: json

  defp add_datacontenttype(%{"datacontenttype" => nil} = json) do
    Map.put(json, "datacontenttype", "application/json")
  end

  defp validate(json) do
    validate_dataschema_presence([], json)
    |> validate_event_schema(json)
    |> validate_dataschema(json)
    |> handle_errors(json)
  end

  defp validate_dataschema_presence(errors, %{"data" => nil}), do: errors

  defp validate_dataschema_presence(errors, %{"data" => _data} = json)
       when is_map_key(json, "dataschema") == false do
    validate_dataschema_presence(errors, Map.put(json, "dataschema", nil))
  end

  defp validate_dataschema_presence(errors, %{"data" => _data, "dataschema" => nil}) do
    add_error(
      errors,
      "Included data without a dataschema. Any data sent through Polyn events must have an associated dataschema."
    )
  end

  defp validate_dataschema_presence(errors, _event), do: errors

  defp validate_event_schema(errors, json) do
    case get_cloud_event_schema(json["specversion"]) do
      nil -> add_error(errors, "Polyn does not recognize specversion #{json["specversion"]}")
      schema -> validate_schema(errors, schema, json)
    end
  end

  defp get_cloud_event_schema(version) do
    try do
      Polyn.CloudEvent.json_schema_for_version(version)
    rescue
      _error ->
        nil
    end
  end

  defp validate_dataschema(errors, json) when is_map_key(json, "dataschema") == false, do: errors
  defp validate_dataschema(errors, %{"dataschema" => nil}), do: errors

  defp validate_dataschema(errors, json) do
    case get_dataschema(json["type"], json["dataschema"]) do
      {:ok, schema} ->
        validate_schema(errors, Jason.decode!(schema), json["data"])

      {:error, _reason} ->
        add_error(errors, "Polyn could not find dataschema #{json["dataschema"]}")
    end
  end

  defp get_dataschema(event_type, dataschema) do
    dataschema = String.replace(dataschema, ":", ".") <> ".json"

    file().read(Path.join(dataschema_dir(), "#{event_type}/#{dataschema}"))
  end

  defp dataschema_dir do
    Path.join(file().cwd!(), "/priv/polyn/schemas")
  end

  defp validate_schema(errors, schema, json) do
    schema = ExJsonSchema.Schema.resolve(schema)

    case ExJsonSchema.Validator.validate(schema, json) do
      :ok ->
        errors

      {:error, json_errors} ->
        Enum.map(json_errors, fn {message, property_path} ->
          "Property: `#{property_path}` - #{message}"
        end)
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
