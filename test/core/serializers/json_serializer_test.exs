defmodule Polyn.Serializers.JSONTest do
  use ExUnit.Case, async: true

  alias Polyn.Event
  alias Polyn.Serializers.JSON

  describe "deserialize/1" do
    # test "validates" do
    #   JSON.deserialize(%{})
    # end
  end

  describe "serialize/1" do
    test "turns non-data event into JSON" do
      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()

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
               "time" => now,
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
          dataschema: "com:foo:user:created:v1:schema:v1"
        )
        |> JSON.serialize()
        |> Jason.decode!()

      assert json["data"] == %{"foo" => "bar"}
      assert json["dataschema"] == "com:foo:user:created:v1:schema:v1"
    end

    test "error if data without dataschema" do
      event =
        Event.new(
          spec_version: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{foo: "bar"}
        )

      assert_raise(
        Polyn.ValidationException,
        "Polyn event #{event.id} included data without a dataschema. Any data sent through Polyn events must have an associated dataschema. []",
        fn ->
          JSON.serialize(event)
        end
      )
    end

    test "error if missing id" do
      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          id: nil,
          spec_version: "1.0.1",
          type: "user.created.v1",
          source: "test"
        )
        |> JSON.serialize()
      end)
    end
  end
end
