defmodule Polyn.Serializers.JSONTest do
  use Polyn.ConnCase, async: true

  alias Polyn.Event
  alias Polyn.SchemaStore
  alias Polyn.Serializers.JSON

  @conn_name :json_serializer_gnat
  @moduletag with_gnat: @conn_name

  @store_name "JSON_SERIALIZER_TEST_SCHEMA_STORE"

  setup do
    SchemaStore.create_store(@conn_name, name: @store_name)

    on_exit(fn ->
      cleanup()
    end)
  end

  describe "deserialize/3" do
    test "turns non-data json into eventt" do
      add_schema("user.created.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "null"}}
      })

      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()

      {:ok, event} =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: Event.full_type("user.created.v1"),
          source: "test",
          time: now,
          data: nil
        }
        |> Jason.encode!()
        |> JSON.deserialize(@conn_name, store_name: @store_name)

      assert %Event{
               id: "foo",
               specversion: "1.0.1",
               type: "com.test.user.created.v1",
               source: "test",
               time: ^now,
               data: nil
             } = event
    end

    test "turns data json into event" do
      add_schema("user.created.v1", %{
        "type" => "object",
        "properties" => %{
          "data" => %{"type" => "object", "properties" => %{"foo" => %{"type" => "string"}}}
        }
      })

      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()

      {:ok, event} =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: Event.full_type("user.created.v1"),
          source: "test",
          time: now,
          data: %{foo: "bar"}
        }
        |> Jason.encode!()
        |> JSON.deserialize(@conn_name, store_name: @store_name)

      assert %Event{
               id: "foo",
               specversion: "1.0.1",
               type: "com.test.user.created.v1",
               source: "test",
               time: ^now,
               data: %{"foo" => "bar"}
             } = event
    end

    test "error if data without dataschema" do
      {:error, message} =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: Event.full_type("user.created.v1"),
          source: "test",
          data: %{foo: "bar"}
        }
        |> Jason.encode!()
        |> JSON.deserialize(@conn_name, store_name: @store_name)

      assert message =~ "Schema for user.created.v1 does not exist."
    end

    test "error if data doesn't match schema" do
      add_schema("user.created.v1", %{
        "type" => "object",
        "properties" => %{
          "data" => %{"type" => "object", "properties" => %{"foo" => %{"type" => "integer"}}}
        }
      })

      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()

      {:error, message} =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: Event.full_type("user.created.v1"),
          source: "test",
          time: now,
          data: %{foo: "bar"}
        }
        |> Jason.encode!()
        |> JSON.deserialize(@conn_name, store_name: @store_name)

      assert message =~ "Polyn event foo from test is not valid"
      assert message =~ "Property: `#/data/foo` - Type mismatch. Expected Integer but got String."
    end

    test "error if data isn't cloudevent" do
      {:error, message} = JSON.deserialize("123", @conn_name, store_name: @store_name)

      assert message =~ "Polyn events need to follow the CloudEvent spec"
      assert message =~ "Expected Object but got Integer"
    end

    test "error if payload is not decodeable" do
      assert {:error, message} = JSON.deserialize("foo", @conn_name, store_name: @store_name)

      assert message =~ "Polyn was unable to decode the following message: \nfoo"
    end
  end

  describe "deserialize!/3" do
    test "raises if invalid" do
      %{message: message} =
        assert_raise(Polyn.ValidationException, fn ->
          %{
            id: "foo",
            specversion: "1.0.1",
            type: Event.full_type("user.created.v1"),
            source: "test",
            data: %{foo: "bar"}
          }
          |> Jason.encode!()
          |> JSON.deserialize!(@conn_name, store_name: @store_name)
        end)

      assert message =~ "Schema for user.created.v1 does not exist."
    end
  end

  describe "serialize!/3" do
    test "turns non-data event into JSON" do
      add_schema("user.created.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "null"}}
      })

      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()
      langversion = System.build_info().version
      version = Polyn.MixProject.version()

      json =
        Event.new(
          specversion: "1.0.1",
          type: Event.full_type("user.created.v1"),
          source: "test",
          time: now
        )
        |> JSON.serialize!(@conn_name, store_name: @store_name)
        |> Jason.decode!()

      assert %{
               "specversion" => "1.0.1",
               "type" => "com.test.user.created.v1",
               "source" => "test",
               "time" => ^now,
               "polyntrace" => [],
               "polyndata" => %{
                 "clientlang" => "elixir",
                 "clientlangversion" => ^langversion,
                 "clientversion" => ^version
               },
               "data" => nil,
               "datacontenttype" => "application/json"
             } = json

      assert UUID.info!(json["id"]) |> Keyword.get(:version) == 4
    end

    test "turns data event into JSON" do
      add_schema("user.created.v1", %{
        "type" => "object",
        "properties" => %{
          "data" => %{"type" => "object", "properties" => %{"foo" => %{"type" => "string"}}}
        }
      })

      json =
        Event.new(
          specversion: "1.0.1",
          type: Event.full_type("user.created.v1"),
          source: "test",
          data: %{"foo" => "bar"}
        )
        |> JSON.serialize!(@conn_name, store_name: @store_name)
        |> Jason.decode!()

      assert json["data"] == %{"foo" => "bar"}
      assert json["datacontenttype"] == "application/json"
    end

    test "works with different datacontenttype than json" do
      add_schema("user.created.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"foo" => "string"}}
      })

      json =
        Event.new(
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          datacontenttype: "application/xml",
          data: "<much wow=\"xml\"/>"
        )
        |> JSON.serialize!(@conn_name, store_name: @store_name)
        |> Jason.decode!()

      assert json["data"] == "<much wow=\"xml\"/>"
      assert json["datacontenttype"] == "application/xml"
    end

    test "error if no dataschema, even without data" do
      event =
        Event.new(
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: nil
        )

      %{message: message} =
        assert_raise(Polyn.ValidationException, fn ->
          JSON.serialize!(event, @conn_name, store_name: @store_name)
        end)

      assert message =~ "Schema for user.created.v1 does not exist"
    end

    test "error if missing id" do
      add_schema("user.created.v1", %{
        "type" => "object",
        "properties" => %{
          "id" => %{"type" => "string", "minLength" => 1},
          "data" => %{"type" => "string"}
        }
      })

      %{message: message} =
        assert_raise(Polyn.ValidationException, fn ->
          Event.new(
            id: nil,
            specversion: "1.0.1",
            type: "user.created.v1",
            source: "test",
            data: "foo"
          )
          |> JSON.serialize!(@conn_name, store_name: @store_name)
        end)

      assert message =~ "Property: `#/id` - Type mismatch. Expected String but got Null."
    end

    test "error if data and schema don't match" do
      add_schema("user.created.v1", %{
        "type" => "object",
        "properties" => %{
          "data" => %{"type" => "string"}
        }
      })

      %{message: message} =
        assert_raise(Polyn.ValidationException, fn ->
          Event.new(
            id: "abc",
            specversion: "1.0.1",
            type: Event.full_type("user.created.v1"),
            source: "test",
            data: nil
          )
          |> JSON.serialize!(@conn_name, store_name: @store_name)
        end)

      assert message =~ "Polyn event abc from test is not valid"
      assert message =~ "Property: `#/data` - Type mismatch. Expected String but got Null."
    end
  end

  defp add_schema(type, schema) do
    SchemaStore.save(@conn_name, type, schema, name: @store_name)
  end

  defp cleanup do
    # Manage connection on our own here, because all supervised processes will be
    # closed by the time `on_exit` runs
    {:ok, pid} = Gnat.start_link()
    SchemaStore.delete_store(pid, name: @store_name)
    Gnat.stop(pid)
  end
end
