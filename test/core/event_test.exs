defmodule Polyn.EventTest do
  use ExUnit.Case, async: true

  alias Polyn.Event

  describe "new/1" do
    test "adds id if none provided" do
      assert %Event{id: id} = Event.new([])
      assert UUID.info!(id) |> Keyword.get(:version) == 4
    end

    test "adds time if none provided" do
      assert %Event{time: time} = Event.new([])
      assert {:ok, %DateTime{}, _offset} = DateTime.from_iso8601(time)
    end

    test "adds source if none provided" do
      assert %Event{source: source} = Event.new([])
      assert source == "com:test:user:backend"
    end

    test "adds polyn_version" do
      assert %Event{polyndata: %{clientversion: version}} = Event.new([])
      assert version == "#{Application.spec(:polyn, :vsn)}"
    end

    test "doesn't add polyn_version if already there" do
      version = "#{Application.spec(:polyn, :vsn)}"
      event = Event.new(%{polyndata: %{"clientversion" => version}})

      assert event.polyndata["clientversion"] == version
      assert event.polyndata[:clientversion] == nil
    end
  end

  describe "full_type/1" do
    test "prefixes domain" do
      assert "com.test.user.created.v1" == Event.full_type("user.created.v1")
    end

    test "ignores existing domain prefix" do
      assert "com.test.user.created.v1" == Event.full_type("com.test.user.created.v1")
    end

    test "raises if invalid type" do
      assert_raise(Polyn.ValidationException, fn ->
        Event.full_type("user created v1")
      end)
    end
  end

  describe "full_source" do
    test "creates source with domain and source_root" do
      assert "com:test:user:backend" == Event.full_source()
    end

    test "raises if invalid name" do
      assert_raise(Polyn.ValidationException, fn ->
        assert "com:test:user:backend:orders" == Event.full_source("*orders*")
      end)
    end

    test "creates source with producer name appended" do
      assert "com:test:user:backend:orders" == Event.full_source("orders")
    end

    test "replaces dots" do
      assert "com:test:user:backend:orders:new" == Event.full_source("orders.new")
    end

    test "uses root if nil" do
      assert "com:test:user:backend" == Event.full_source(nil)
    end

    test "does not duplicate root" do
      assert "com:test:user:backend" == Event.full_source("com:test:user:backend")
    end

    test "does not duplicate root with custom source" do
      assert "com:test:user:backend:orders:new" ==
               Event.full_source("com:test:user:backend:orders:new")
    end
  end
end
