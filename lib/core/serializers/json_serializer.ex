defmodule Polyn.Serializers.JSON do
  @spec deserialize(json :: binary()) :: {:ok, any} | {:error, any()}
  def deserialize(json) do
    data = Jason.decode!(json)
    schema = ExJsonSchema.Schema.resolve(Polyn.CloudEvent.V_1_0_1.json_schema())

    case ExJsonSchema.Validator.validate(schema, data) do
      :ok -> {:ok, data}
      error -> error
    end
  end

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
      |> validate_json_schema(json)

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

  defp validate_json_schema(errors, json) do
    schema =
      Polyn.CloudEvent.json_schema_for_version(json["specversion"])
      |> ExJsonSchema.Schema.resolve()

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
