defmodule Polyn.Serializers.JSONTest do
  use ExUnit.Case, async: true

  alias Polyn.Event
  alias Polyn.SchemaStore
  alias Polyn.Serializers.JSON

  @store_name "JSON_SERIALIZER_TEST_SCHEMA_STORE"

  setup do
    start_supervised!(
      {SchemaStore,
       [
         store_name: @store_name,
         connection_name: :foo,
         schemas: %{
           "foo.created.v1" =>
             Jason.encode!(%{
               "type" => "object",
               "properties" => %{"data" => %{"type" => "null"}}
             }),
           "user.xml.v1" =>
             Jason.encode!(%{
               "type" => "object",
               "properties" => %{"data" => %{"type" => "string"}}
             }),
           "user.created.v1" =>
             Jason.encode!(%{
               "type" => "object",
               "properties" => %{
                 "id" => %{"type" => "string", "minLength" => 1},
                 "data" => %{
                   "type" => "object",
                   "properties" => %{"foo" => %{"type" => "string"}}
                 }
               }
             })
         }
       ]}
    )

    :ok
  end

  describe "deserialize/3" do
    test "turns non-data json into eventt" do
      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()

      {:ok, event} =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: Event.full_type("foo.created.v1"),
          source: "test",
          time: now,
          data: nil
        }
        |> Jason.encode!()
        |> JSON.deserialize(store_name: @store_name)

      assert %Event{
               id: "foo",
               specversion: "1.0.1",
               type: "com.test.foo.created.v1",
               source: "test",
               time: ^now,
               data: nil
             } = event
    end

    test "turns data json into event" do
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
        |> JSON.deserialize(store_name: @store_name)

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
          type: Event.full_type("not.a.schema.v1"),
          source: "test",
          data: %{foo: "bar"}
        }
        |> Jason.encode!()
        |> JSON.deserialize(store_name: @store_name)

      assert message =~ "Schema for not.a.schema.v1 does not exist."
    end

    test "error if data doesn't match schema" do
      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()

      {:error, message} =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: Event.full_type("user.created.v1"),
          source: "test",
          time: now,
          data: %{foo: 1}
        }
        |> Jason.encode!()
        |> JSON.deserialize(store_name: @store_name)

      assert message =~ "Polyn event foo from test is not valid"
      assert message =~ "Property: `#/data/foo` - Type mismatch. Expected String but got Integer."
    end

    test "error if invalid type" do
      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()

      {:error, message} =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: "user created v1",
          source: "test",
          time: now,
          data: %{foo: "bar"}
        }
        |> Jason.encode!()
        |> JSON.deserialize(store_name: @store_name)

      assert message =~ "Event names must be lowercase, alphanumeric and dot separated"
    end

    test "error if data isn't cloudevent" do
      {:error, message} = JSON.deserialize("123", store_name: @store_name)

      assert message =~ "Polyn events need to follow the CloudEvent spec"
      assert message =~ "Expected Object but got Integer"
    end

    test "error if payload is not decodeable" do
      assert {:error, message} = JSON.deserialize("foo", store_name: @store_name)

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
            type: Event.full_type("not.a.schema.v1"),
            source: "test",
            data: %{foo: "bar"}
          }
          |> Jason.encode!()
          |> JSON.deserialize!(store_name: @store_name)
        end)

      assert message =~ "Schema for not.a.schema.v1 does not exist."
    end
  end

  describe "serialize!/3" do
    test "turns non-data event into JSON" do
      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()
      langversion = System.build_info().version
      version = Polyn.MixProject.version()

      json =
        Event.new(
          specversion: "1.0.1",
          type: Event.full_type("foo.created.v1"),
          source: "test",
          time: now
        )
        |> JSON.serialize!(store_name: @store_name)
        |> Jason.decode!()

      assert %{
               "specversion" => "1.0.1",
               "type" => "com.test.foo.created.v1",
               "source" => "test",
               "time" => ^now,
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
      json =
        Event.new(
          specversion: "1.0.1",
          type: Event.full_type("user.created.v1"),
          source: "test",
          data: %{"foo" => "bar"}
        )
        |> JSON.serialize!(store_name: @store_name)
        |> Jason.decode!()

      assert json["data"] == %{"foo" => "bar"}
      assert json["datacontenttype"] == "application/json"
    end

    test "works with different datacontenttype than json" do
      json =
        Event.new(
          specversion: "1.0.1",
          type: "user.xml.v1",
          source: "test",
          datacontenttype: "application/xml",
          data: "<much wow=\"xml\"/>"
        )
        |> JSON.serialize!(store_name: @store_name)
        |> Jason.decode!()

      assert json["data"] == "<much wow=\"xml\"/>"
      assert json["datacontenttype"] == "application/xml"
    end

    test "error if no dataschema, even without data" do
      event =
        Event.new(
          specversion: "1.0.1",
          type: "not.a.schema.v1",
          source: "test",
          data: nil
        )

      %{message: message} =
        assert_raise(Polyn.ValidationException, fn ->
          JSON.serialize!(event, store_name: @store_name)
        end)

      assert message =~ "Schema for not.a.schema.v1 does not exist"
    end

    test "error if missing id" do
      %{message: message} =
        assert_raise(Polyn.ValidationException, fn ->
          Event.new(
            id: nil,
            specversion: "1.0.1",
            type: "user.created.v1",
            source: "test",
            data: "foo"
          )
          |> JSON.serialize!(store_name: @store_name)
        end)

      assert message =~ "Property: `#/id` - Type mismatch. Expected String but got Null."
    end

    test "error if data and schema don't match" do
      %{message: message} =
        assert_raise(Polyn.ValidationException, fn ->
          Event.new(
            id: "abc",
            specversion: "1.0.1",
            type: Event.full_type("user.created.v1"),
            source: "test",
            data: nil
          )
          |> JSON.serialize!(store_name: @store_name)
        end)

      assert message =~ "Polyn event abc from test is not valid"
      assert message =~ "Property: `#/data` - Type mismatch. Expected Object but got Null."
    end
  end
end
