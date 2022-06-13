defmodule Polyn.ProducerTest do
  use Polyn.ConnCase, async: true

  alias Polyn.Event
  alias Polyn.Producer
  alias Polyn.SchemaStore

  @conn_name :producer_gnat
  @moduletag with_gnat: @conn_name

  @store_name "PRODUCER_TEST_SCHEMA_STORE"

  setup do
    SchemaStore.create_store(@conn_name, name: @store_name)
  end

  test "pub/3 adds a new event to the server" do
    add_schema("user.created.v1", %{
      "type" => "object",
      "properties" => %{"data" => %{"type" => "string"}}
    })

    Gnat.sub(@conn_name, self(), "user.created.v1")
    Producer.pub(@conn_name, "user.created.v1", "foo", store_name: @store_name)

    data = get_message()
    assert data["data"] == "foo"
    assert data["datacontenttype"] == "application/json"
    assert data["source"] == "com:test:user:backend"
    assert data["specversion"] == "1.0.1"
    assert data["type"] == "com.test.user.created.v1"
    assert data["polyntrace"] == []

    delete_store()
  end

  test "pub/3 can include extra `source` info" do
    add_schema("user.created.v1", %{
      "type" => "object",
      "properties" => %{"data" => %{"type" => "string"}}
    })

    Gnat.sub(@conn_name, self(), "user.created.v1")
    Producer.pub(@conn_name, "user.created.v1", "foo", store_name: @store_name, source: "orders")

    data = get_message()
    assert data["source"] == "com:test:user:backend:orders"

    delete_store()
  end

  test "pub/3 builds up polyntrace if :triggered_by is supplied" do
    add_schema("user.created.v1", %{
      "type" => "object",
      "properties" => %{"data" => %{"type" => "string"}}
    })

    Gnat.sub(@conn_name, self(), "user.created.v1")

    trigger_event =
      Event.new(
        type: Event.full_type("user.form.finished.v1"),
        polyntrace: [
          %{
            "id" => Event.new_event_id(),
            "time" => Event.new_timestamp(),
            "type" => Event.full_type("user.entered.site.v1")
          }
        ]
      )

    Producer.pub(@conn_name, "user.created.v1", "foo",
      store_name: @store_name,
      triggered_by: trigger_event
    )

    data = get_message()

    assert data["polyntrace"] == [
             Enum.at(trigger_event.polyntrace, 0),
             %{
               "id" => trigger_event.id,
               "time" => trigger_event.time,
               "type" => trigger_event.type
             }
           ]

    delete_store()
  end

  test "pub/3 raises if doesn't match schema" do
    add_schema("user.created.v1", %{
      "type" => "object",
      "properties" => %{"data" => %{"type" => "string"}}
    })

    assert_raise(Polyn.ValidationException, fn ->
      Producer.pub(@conn_name, "user.created.v1", 100, store_name: @store_name, source: "orders")
    end)

    delete_store()
  end

  defp add_schema(type, schema) do
    SchemaStore.save(@conn_name, type, schema, name: @store_name)
  end

  defp get_message do
    receive do
      {:msg, %{body: body}} ->
        Jason.decode!(body)
    after
      100 ->
        raise "no message"
    end
  end

  defp delete_store do
    SchemaStore.delete_store(@conn_name, name: @store_name)
  end
end
