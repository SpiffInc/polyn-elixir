defmodule Polyn.SchemaStoreTest do
  use Polyn.ConnCase, async: true

  alias Jetstream.API.KV
  alias Polyn.SchemaStore

  @conn_name :schema_store_gnat
  @moduletag with_gnat: @conn_name

  @store_name "POLYN_SCHEMAS_SCHEMA_STORE_TEST"

  setup do
    on_exit(fn ->
      cleanup(fn pid ->
        delete_store(pid)
      end)
    end)
  end

  describe "start_link/1" do
    test "loads schema on init" do
      assert :ok = SchemaStore.create_store(@conn_name, name: @store_name)
      KV.put_value(@conn_name, @store_name, "foo", "bar")

      store =
        start_supervised!(
          {SchemaStore,
           store_name: @store_name, connection_name: @conn_name, name: String.to_atom(@store_name)}
        )

      assert SchemaStore.get_schemas(store) == %{"foo" => "bar"}
    end
  end

  describe "create_store/0" do
    test "creates a store" do
      assert :ok = SchemaStore.create_store(@conn_name, name: @store_name)
    end

    test "called multiple times won't break" do
      assert :ok = SchemaStore.create_store(@conn_name, name: @store_name)
      assert :ok = SchemaStore.create_store(@conn_name, name: @store_name)
    end

    test "handles when store already exists with different config" do
      KV.create_bucket(@conn_name, @store_name, description: "foo")
      assert :ok = SchemaStore.create_store(@conn_name, name: @store_name)
    end
  end

  describe "save/2" do
    setup :init_store

    test "persists a new schema", %{store: store} do
      assert :ok =
               SchemaStore.save(
                 store,
                 "foo.bar",
                 %{type: "null"}
               )

      assert SchemaStore.get(store, "foo.bar") == %{"type" => "null"}
    end

    test "updates already existing", %{store: store} do
      assert :ok =
               SchemaStore.save(
                 store,
                 "foo.bar",
                 %{type: "string"}
               )

      assert :ok =
               SchemaStore.save(
                 store,
                 "foo.bar",
                 %{type: "null"}
               )

      assert SchemaStore.get(store, "foo.bar") == %{"type" => "null"}
    end

    test "error if not a JSONSchema document", %{store: store} do
      assert_raise(
        Polyn.SchemaException,
        "Schemas must be valid JSONSchema documents, got %{\"type\" => \"not-a-valid-type\"}",
        fn ->
          SchemaStore.save(store, "foo.bar", %{"type" => "not-a-valid-type"})
        end
      )
    end
  end

  describe "delete/1" do
    setup :init_store

    test "deletes a schema", %{store: store} do
      assert :ok =
               SchemaStore.save(
                 store,
                 "foo.bar",
                 %{
                   type: "null"
                 }
               )

      assert :ok = SchemaStore.delete(store, "foo.bar")

      assert SchemaStore.get(store, "foo.bar") == nil
    end

    test "deletes a schema that doesn't exist", %{store: store} do
      assert :ok = SchemaStore.delete(store, "foo.bar")

      assert SchemaStore.get(store, "foo.bar") == nil
    end
  end

  describe "get/2" do
    setup :init_store

    test "returns nil if not found", %{store: store} do
      assert SchemaStore.get(store, "foo.bar") == nil
    end
  end

  defp init_store(context) do
    SchemaStore.create_store(@conn_name, name: @store_name)

    store =
      start_supervised!(
        {SchemaStore,
         store_name: @store_name,
         connection_name: @conn_name,
         schemas: %{},
         name: String.to_atom(@store_name)}
      )

    Map.put(context, :store, store)
  end

  defp delete_store(pid) do
    :ok = SchemaStore.delete_store(pid, name: @store_name)
  end
end
