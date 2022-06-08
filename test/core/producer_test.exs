defmodule Polyn.ProducerTest do
  use ExUnit.Case, async: true

  alias Polyn.Connection
  alias Polyn.Producer
  alias Polyn.SchemaStore

  @store_name "PRODUCER_TEST_SCHEMA_STORE"

  setup do
    SchemaStore.create_store(name: @store_name)

    on_exit(fn ->
      SchemaStore.delete_store(name: @store_name)
    end)
  end

  test "pub/3 adds a new event to the server" do
    add_schema("user.created.v1", %{
      "type" => "object",
      "properties" => %{"data" => %{"type" => "string"}}
    })

    Gnat.sub(Connection.name(), self(), "user.created.v1")
    Producer.pub("user.created.v1", "foo", store_name: @store_name)

    receive do
      {:msg, %{body: body}} ->
        data = Jason.decode!(body)
        assert data["data"] == "foo"
        assert data["datacontenttype"] == "application/json"
        assert data["source"] == "com:test:user:backend"
        assert data["specversion"] == "1.0.1"
        assert data["type"] == "com.test.user.created.v1"
    after
      100 ->
        raise "no message"
    end
  end

  test "pub/3 can include extra `source` info" do
    add_schema("user.created.v1", %{
      "type" => "object",
      "properties" => %{"data" => %{"type" => "string"}}
    })

    Gnat.sub(Connection.name(), self(), "user.created.v1")
    Producer.pub("user.created.v1", "foo", store_name: @store_name, source: "orders")

    receive do
      {:msg, %{body: body}} ->
        data = Jason.decode!(body)
        assert data["source"] == "com:test:user:backend:orders"
    after
      100 ->
        raise "no message"
    end
  end

  test "pub/3 raises if doesn't match schema" do
    add_schema("user.created.v1", %{
      "type" => "object",
      "properties" => %{"data" => %{"type" => "string"}}
    })

    Gnat.sub(Connection.name(), self(), "user.created.v1")

    assert_raise(Polyn.ValidationException, fn ->
      Producer.pub("user.created.v1", 100, store_name: @store_name, source: "orders")
    end)
  end

  defp add_schema(type, schema) do
    SchemaStore.save(type, schema, name: @store_name)
  end
end
