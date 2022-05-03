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

  defp serialize_field(data, {:spec_version, value}) do
    Map.put(data, "specversion", value)
  end

  defp serialize_field(data, {:client, value}) do
    Map.put(data, "polynclient", value)
  end

  defp serialize_field(data, {:trace, value}) do
    Map.put(data, "polyntrace", value)
  end

  defp serialize_field(data, {_key, nil}), do: data

  defp serialize_field(data, {key, value}) do
    Map.put(data, Atom.to_string(key), value)
  end

  defp validate(json) do
    schema =
      Polyn.CloudEvent.json_schema_for_version(json["specversion"])
      |> ExJsonSchema.Schema.resolve()

    case ExJsonSchema.Validator.validate(schema, json) do
      :ok -> {:ok, json}
      {:error, message} -> raise Polyn.ValidationException, inspect(message)
    end

    if Map.has_key?(json, "data") and !json["dataschema"] do
      raise Polyn.ValidationException,
            "Polyn event #{json["id"]} included data without a dataschema. Any data sent through Polyn events must have an associated dataschema. #{inspect(json["polyntrace"])}"
    else
      json
    end
  end
end
