defmodule Polyn.Serializers.JSON do
  @moduledoc """
  JSON Serializer for Polyn events. Functions will raise if
  inconsistencies are found
  """

  # @doc """
  # Convert a JSON payload into a Polyn.Event struct
  # Raises an error if json is not valid
  # """
  # @spec deserialize(json :: binary()) :: Polyn.Event.t()
  # def deserialize(json) do
  #   data = Jason.decode!(json) |> validate()
  # end

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
    |> validate()
    |> Jason.encode!()
  end

  defp serialize_field(data, {key, value}) do
    Map.put(data, Atom.to_string(key), value)
  end

  defp validate(json) do
    errors =
      validate_dataschema_presence([], json)
      |> validate_event_schema(json)

    if Enum.empty?(errors) do
      json
    else
      errors = add_error(errors, "Polyn event #{json["id"]} is not valid")
      errors = errors ++ ["Event data: #{inspect(json)}"]
      errors = Enum.join(errors, "\n")
      raise Polyn.ValidationException, errors
    end
  end

  defp validate_dataschema_presence(errors, %{"data" => nil}), do: errors

  defp validate_dataschema_presence(errors, %{"data" => _data, "dataschema" => nil}) do
    add_error(
      errors,
      "Included data without a dataschema. Any data sent through Polyn events must have an associated dataschema."
    )
  end

  defp validate_dataschema_presence(errors, _event), do: errors

  defp validate_event_schema(errors, json) do
    schema = get_cloud_event_schema(json["specversion"])
    validate_schema(errors, schema, json)
  end

  defp get_cloud_event_schema(version) do
    try do
      Polyn.CloudEvent.json_schema_for_version(version)
      |> ExJsonSchema.Schema.resolve()
    rescue
      _error ->
        nil
    end
  end

  defp validate_schema(errors, nil, json) do
    add_error(errors, "Polyn does not recognize specversion #{json["specversion"]}")
  end

  defp validate_schema(errors, schema, json) do
    case ExJsonSchema.Validator.validate(schema, json) do
      :ok ->
        errors

      {:error, json_errors} ->
        json_errors = Enum.map(json_errors, &elem(&1, 0))
        Enum.concat(errors, json_errors)
    end
  end

  defp add_error(errors, error) do
    [error | errors]
  end
end
