defmodule Polyn.Serializers.JSONTest do
  use ExUnit.Case, async: true

  alias Polyn.Event
  alias Polyn.Serializers.JSON

  @moduletag :tmp_dir

  describe "deserialize/1" do
    test "turns non-data json into event", %{tmp_dir: tmp_dir} do
      add_dataschema(tmp_dir, "user.created.v1", "user.created.v1.schema.v1.json", """
      {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "com:foo:user:created:v1:schema:v1",
        "type": "null"
      }
      """)

      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()

      event =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: Event.type("user.created"),
          source: "test",
          time: now,
          data: nil,
          dataschema: Event.type("user.created") |> Event.dataschema()
        }
        |> Jason.encode!()
        |> JSON.deserialize(dataschemas_dir: tmp_dir)

      assert %Event{
               id: "foo",
               specversion: "1.0.1",
               type: "com.test.user.created.v1",
               source: "test",
               time: ^now,
               data: nil,
               dataschema: "com:test:user:created:v1:schema:v1"
             } = event
    end

    test "turns data json into event", %{tmp_dir: tmp_dir} do
      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()

      add_dataschema(tmp_dir, "user.created.v1", "user.created.v1.schema.v1.json", """
      {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "com:test:user:created:v1:schema:v1",
        "type": "object",
        "properties": {"foo": {"type": "string"}},
        "required": ["foo"]
      }
      """)

      event =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: Event.type("user.created"),
          source: "test",
          time: now,
          dataschema: Event.type("user:created") |> Event.dataschema(),
          data: %{foo: "bar"}
        }
        |> Jason.encode!()
        |> JSON.deserialize(dataschemas_dir: tmp_dir)

      assert %Event{
               id: "foo",
               specversion: "1.0.1",
               type: "com.test.user.created.v1",
               source: "test",
               time: ^now,
               data: %{"foo" => "bar"},
               dataschema: "com:test:user:created:v1:schema:v1"
             } = event
    end

    test "error if data without dataschema", %{tmp_dir: tmp_dir} do
      json =
        %{
          id: "foo",
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{foo: "bar"}
        }
        |> Jason.encode!()

      assert_raise(Polyn.ValidationException, fn ->
        JSON.deserialize(json, dataschemas_dir: tmp_dir)
      end)
    end
  end

  describe "serialize/1" do
    test "turns non-data event into JSON", %{tmp_dir: tmp_dir} do
      add_dataschema(tmp_dir, "user.created.v1", "user.created.v1.schema.v1.json", """
      {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "com:foo:user:created:v1:schema:v1",
        "type": "null"
      }
      """)

      now = NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601()
      langversion = System.build_info().version
      version = "#{Application.spec(:polyn, :vsn)}"

      json =
        Event.new(
          specversion: "1.0.1",
          type: Event.type("user.created"),
          source: "test",
          time: now,
          dataschema: Event.type("user:created") |> Event.dataschema()
        )
        |> JSON.serialize(dataschemas_dir: tmp_dir)
        |> Jason.decode!()

      assert %{
               "specversion" => "1.0.1",
               "type" => "com.test.user.created.v1",
               "source" => "test",
               "time" => ^now,
               "polyntrace" => [],
               "polynclient" => %{
                 "lang" => "elixir",
                 "langversion" => ^langversion,
                 "version" => ^version
               },
               "data" => nil,
               "dataschema" => "com:test:user:created:v1:schema:v1",
               "datacontenttype" => "application/json"
             } = json

      assert UUID.info!(json["id"]) |> Keyword.get(:version) == 4
    end

    test "turns data event into JSON", %{tmp_dir: tmp_dir} do
      add_dataschema(tmp_dir, "user.created.v1", "user.created.v1.schema.v1.json", """
      {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "com:foo:user:created:v1:schema:v1",
        "type": "object",
        "properties": {"foo": {"type": "string"}},
        "required": ["foo"]
      }
      """)

      json =
        Event.new(
          specversion: "1.0.1",
          type: Event.type("user.created"),
          source: "test",
          data: %{foo: "bar"},
          dataschema: Event.type("user:created") |> Event.dataschema()
        )
        |> JSON.serialize(dataschemas_dir: tmp_dir)
        |> Jason.decode!()

      assert json["data"] == %{"foo" => "bar"}
      assert json["dataschema"] == "com:test:user:created:v1:schema:v1"
      assert json["datacontenttype"] == "application/json"
    end

    test "works with different datacontenttype than json", %{tmp_dir: tmp_dir} do
      add_dataschema(tmp_dir, "user.created.v1", "user.created.v1.schema.v1.json", """
      {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "com:foo:user:created:v1:schema:v1",
        "type": "string",
        "contentMediaType": "application/xml"
      }
      """)

      json =
        Event.new(
          specversion: "1.0.1",
          type: Event.type("user.created"),
          source: "test",
          dataschema: Event.type("user.created") |> Event.dataschema(),
          datacontenttype: "application/xml",
          data: "<much wow=\"xml\"/>"
        )
        |> JSON.serialize(dataschemas_dir: tmp_dir)
        |> Jason.decode!()

      assert json["data"] == "<much wow=\"xml\"/>"
      assert json["datacontenttype"] == "application/xml"
    end

    test "error if no dataschema, even without data", %{tmp_dir: tmp_dir} do
      event =
        Event.new(
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: nil
        )

      assert_raise(Polyn.ValidationException, fn ->
        JSON.serialize(event, dataschemas_dir: tmp_dir)
      end)
    end

    test "error if missing id", %{tmp_dir: tmp_dir} do
      add_dataschema(tmp_dir, "user.created.v1", "com.foo.user.created.v1.schema.v1.json", """
      {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "com:foo:user:created:v1:schema:v1",
        "type": "string"
      }
      """)

      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          id: nil,
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          dataschema: "com:foo:user:created:v1:schema:v1",
          data: "foo"
        )
        |> JSON.serialize(dataschemas_dir: tmp_dir)
      end)
    end

    test "error if unknown specversion", %{tmp_dir: tmp_dir} do
      add_dataschema(tmp_dir, "user.created.v1", "com.foo.user.created.v1.schema.v1.json", """
      {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "com:foo:user:created:v1:schema:v1",
        "type": "string"
      }
      """)

      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          specversion: "foo",
          type: "user.created.v1",
          source: "test",
          dataschema: "com:foo:user:created:v1:schema:v1",
          data: "foo"
        )
        |> JSON.serialize(dataschema_dir: tmp_dir)
      end)
    end

    test "error if dataschema doesn't exist", %{tmp_dir: tmp_dir} do
      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{foo: "bar"},
          dataschema: "com:foo:user:created:v1:schema:v1"
        )
        |> JSON.serialize(dataschemas_dir: tmp_dir)
      end)
    end

    test "error if data doesnt match dataschema", %{tmp_dir: tmp_dir} do
      add_dataschema(tmp_dir, "user.created.v1", "com.foo.user.created.v1.schema.v1.json", """
      {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "com:foo:user:created:v1:schema:v1",
        "type": "object",
        "properties": {"foo": {"type": "string"}},
        "required": ["foo"]
      }
      """)

      assert_raise(Polyn.ValidationException, fn ->
        Event.new(
          specversion: "1.0.1",
          type: "user.created.v1",
          source: "test",
          data: %{"foo" => 10},
          dataschema: "com:foo:user:created:v1:schema:v1"
        )
        |> JSON.serialize(dataschemas_dir: tmp_dir)
      end)
    end
  end

  defp add_dataschema(dir, event, schema_name, content) do
    path = Path.join(dir, event)
    File.mkdir_p!(path)
    File.write!(Path.join(path, schema_name), content)
  end
end
