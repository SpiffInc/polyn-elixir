defmodule Polyn.Serializers.JSONTest do
  use ExUnit.Case, async: true

  alias Polyn.Event
  alias Polyn.Serializers.JSON

  describe "deserialize/1" do
    test "validates" do
      JSON.deserialize(%{})
    end
  end

  describe "serialize/1" do
    test "turns non-data event into JSON" do
      now = NaiveDateTime.utc_now()

      json =
        Event.new(
          spec_version: "1.0.1",
          type: "user.created.v1",
          source: "test",
          time: now
        )
        |> JSON.serialize()
        |> Jason.decode!()

      assert %{
               "specversion" => "1.0.1",
               "type" => "user.created.v1",
               "source" => "test",
               "time" => NaiveDateTime.to_iso8601(now),
               "polyntrace" => [],
               "polynclient" => %{
                 "lang" => "elixir",
                 "langversion" => System.build_info().version,
                 "version" => Polyn.MixProject.version()
               }
             } == Map.delete(json, "id")

      assert UUID.info!(json["id"]) |> Keyword.get(:version) == 4
    end

    test "turns data event into JSON" do
      json =
        Event.new(
          spec_version: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{foo: "bar"},
          dataschema: "com.foo.user.created.v1.schema.v1.json"
        )
        |> JSON.serialize()
        |> Jason.decode!()

      assert json["data"] == %{"foo" => "bar"}
      assert json["dataschema"] == "com.foo.user.created.v1.schema.v1.json"
    end

    test "error if data without dataschema" do
      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          spec_version: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{foo: "bar"}
        )
        |> JSON.serialize()
      end)
    end
  end
end
