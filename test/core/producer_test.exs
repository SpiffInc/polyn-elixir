defmodule Polyn.ProducerTest do
  use ExUnit.Case, async: true

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
    Producer.pub("user.created.v1", "")
  end
end
