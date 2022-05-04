defmodule Polyn.Serializers.JSONTest do
  use ExUnit.Case, async: true

  alias Polyn.Event
  alias Polyn.Serializers.JSON
  alias Polyn.FileMock

  import Mox

  setup :verify_on_exit!

  describe "deserialize/1" do
    # test "validates" do
    #   JSON.deserialize(%{})
    # end
  end

  describe "serialize/1" do
    test "turns non-data event into JSON" do
      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()
      langversion = System.build_info().version
      version = Polyn.MixProject.version()

      json =
        Event.new(
          specversion: "1.0.1",
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
               "time" => ^now,
               "polyntrace" => [],
               "polynclient" => %{
                 "lang" => "elixir",
                 "langversion" => ^langversion,
                 "version" => ^version
               }
             } = json

      assert UUID.info!(json["id"]) |> Keyword.get(:version) == 4
    end

    test "turns data event into JSON" do
      expect(FileMock, :cwd!, fn -> "my_app" end)

      expect(
        FileMock,
        :read,
        fn "my_app/priv/polyn/schemas/user.created.v1/com.foo.user.created.v1.schema.v1.json" ->
          {:ok,
           Jason.encode!(%{
             "$schema" => "http://json-schema.org/draft-07/schema",
             "$id" => "com:foo:user:created:v1:schema:v1",
             "type" => "object",
             "properties" => %{"foo" => %{"type" => "string"}},
             "required" => ["foo"]
           })}
        end
      )

      json =
        Event.new(
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{"foo" => "bar"},
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
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{foo: "bar"}
        )

      assert_raise(Polyn.ValidationException, fn -> JSON.serialize(event) end)
    end

    test "error if missing id" do
      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          id: nil,
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test"
        )
        |> JSON.serialize()
      end)
    end

    test "error if unknown specversion" do
      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          specversion: "foo",
          type: "user.created.v1",
          source: "test"
        )
        |> JSON.serialize()
      end)
    end

    test "error if dataschema doesn't exist" do
      expect(FileMock, :cwd!, fn -> "my_app" end)

      expect(
        FileMock,
        :read,
        fn "my_app/priv/polyn/schemas/user.created.v1/com.foo.user.created.v1.schema.v1.json" ->
          {:error, "not found"}
        end
      )

      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{foo: "bar"},
          dataschema: "com:foo:user:created:v1:schema:v1"
        )
        |> JSON.serialize()
      end)
    end

    test "error if data doesnt match dataschema" do
      expect(FileMock, :cwd!, fn -> "my_app" end)

      expect(
        FileMock,
        :read,
        fn "my_app/priv/polyn/schemas/user.created.v1/com.foo.user.created.v1.schema.v1.json" ->
          {:ok,
           Jason.encode!(%{
             "$schema" => "http://json-schema.org/draft-07/schema",
             "$id" => "com:foo:user:created:v1:schema:v1",
             "type" => "object",
             "properties" => %{"foo" => %{"type" => "string"}},
             "required" => ["foo"]
           })}
        end
      )

      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{"foo" => 10},
          dataschema: "com:foo:user:created:v1:schema:v1"
        )
        |> JSON.serialize()
      end)
    end
  end
end
