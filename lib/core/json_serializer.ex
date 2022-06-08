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
  @spec deserialize(json :: binary()) :: Polyn.Event.t()
  def deserialize(json, opts \\ []) do
    Jason.decode!(json) |> validate(opts) |> to_event()
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
  def serialize(%Event{} = event, opts \\ []) do
    Map.from_struct(event)
    |> Enum.reduce(%{}, fn field, acc ->
      serialize_field(acc, field)
    end)
    |> add_datacontenttype()
    |> validate(opts)
    |> Jason.encode!()
  end

  defp serialize_field(data, {key, value}) do
    Map.put(data, Atom.to_string(key), value)
  end

  defp add_datacontenttype(%{"datacontenttype" => nil} = json) do
    Map.put(json, "datacontenttype", "application/json")
  end

  defp add_datacontenttype(json), do: json

  defp validate(json, opts) do
    get_schema(json, opts)
    |> validate_schema(json)
    |> handle_errors(json)
  end

  defp get_schema(json, opts) do
    case SchemaStore.get(json["type"], name: store_name(opts)) do
      nil ->
        raise Polyn.SchemaException,
              "Schema for #{json["type"]} does not exist. Make sure it's " <>
                "been added to your `events` codebase and has been loaded into the schema store on your NATS " <>
                "server"

      schema ->
        ExJsonSchema.Schema.resolve(schema)
    end
  end

  defp validate_schema(schema, json) do
    case ExJsonSchema.Validator.validate(schema, json) do
      :ok ->
        []

      {:error, json_errors} ->
        Enum.map(json_errors, fn {message, property_path} ->
          "Property: `#{property_path}` - #{message}"
        end)
    end
  end

  defp handle_errors([], json), do: json

  defp handle_errors(errors, json) do
    errors = add_error(errors, "Polyn event #{json["id"]} from #{json["source"]} is not valid")
    errors = errors ++ ["Event data: #{inspect(json)}"]
    errors = Enum.join(errors, "\n")
    raise Polyn.ValidationException, errors
  end

  defp add_error(errors, error) do
    [error | errors]
  end

  defp store_name(opts) do
    Keyword.get(opts, :store_name, SchemaStore.store_name())
  end
end
