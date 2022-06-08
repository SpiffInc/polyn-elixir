defmodule Polyn.SchemaStoreTest do
  use ExUnit.Case, async: true

  alias Polyn.SchemaStore

  @store_name "POLYN_SCHEMAS_SCHEMA_STORE_TEST"

  setup do
    on_exit(fn ->
      SchemaStore.delete_store(name: @store_name)
    end)

    :ok
  end

  describe "create_store/0" do
    test "creates a store" do
      assert :ok = SchemaStore.create_store(name: @store_name)
    end

    test "called multiple times won't break" do
      assert :ok = SchemaStore.create_store(name: @store_name)
      assert :ok = SchemaStore.create_store(name: @store_name)
    end

    test "handles when store already exists with different config" do
      Jetstream.API.KV.create_bucket(Polyn.Connection.name(), @store_name, description: "foo")
      assert :ok = SchemaStore.create_store(name: @store_name)
    end
  end

  describe "save/2" do
    setup :init_store

    test "persists a new schema" do
      assert :ok =
               SchemaStore.save(
                 "foo.bar",
                 %{type: "null"},
                 name: @store_name
               )

      assert SchemaStore.get("foo.bar", name: @store_name) == %{"type" => "null"}
    end

    test "updates already existing" do
      assert :ok =
               SchemaStore.save(
                 "foo.bar",
                 %{type: "string"},
                 name: @store_name
               )

      assert :ok =
               SchemaStore.save(
                 "foo.bar",
                 %{type: "null"},
                 name: @store_name
               )

      assert SchemaStore.get("foo.bar", name: @store_name) == %{"type" => "null"}
    end

    test "error if not a JSONSchema document" do
      assert_raise(
        Polyn.SchemaException,
        "Schemas must be valid JSONSchema documents, got %{\"type\" => \"not-a-valid-type\"}",
        fn ->
          SchemaStore.save("foo.bar", %{"type" => "not-a-valid-type"}, name: @store_name)
        end
      )
    end
  end

  describe "delete/1" do
    setup :init_store

    test "deletes a schema" do
      assert :ok =
               SchemaStore.save(
                 "foo.bar",
                 %{
                   type: "null"
                 },
                 name: @store_name
               )

      assert :ok =
               SchemaStore.delete("foo.bar",
                 name: @store_name
               )

      assert SchemaStore.get("foo.bar",
               name: @store_name
             ) == nil
    end

    test "deletes a schema that doesn't exist" do
      assert :ok =
               SchemaStore.delete("foo.bar",
                 name: @store_name
               )

      assert SchemaStore.get("foo.bar",
               name: @store_name
             ) == nil
    end
  end

  describe "get/1" do
    setup :init_store

    test "returns nil if not found" do
      assert SchemaStore.get("foo.bar",
               name: @store_name
             ) == nil
    end

    test "raises if store not found" do
      SchemaStore.delete_store(name: @store_name)

      %{message: message} =
        assert_raise(
          Polyn.SchemaException,
          fn ->
            SchemaStore.get("foo.bar", name: @store_name)
          end
        )

      assert message =~ "The Schema Store has not been setup on your NATS server"
    end
  end

  defp init_store(context) do
    SchemaStore.create_store(name: @store_name)
    context
  end
end
