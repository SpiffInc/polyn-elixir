defmodule Polyn.EventTest do
  use ExUnit.Case, async: true

  alias Polyn.Event

  test "new/1 adds id if none provided" do
    assert %Event{id: id} = Event.new([])
    assert UUID.info!(id) |> Keyword.get(:version) == 4
  end

  test "new/1 adds time if none provided" do
    assert %Event{time: time} = Event.new([])
    assert {:ok, %DateTime{}, _offset} = DateTime.from_iso8601(time)
  end

  test "new/1 adds source if none provided" do
    assert %Event{source: source} = Event.new([])
    assert source == "com:test:user:backend"
  end

  test "new/1 adds polyn_version" do
    assert %Event{polynclient: %{version: version}} = Event.new([])
    assert version == "#{Application.spec(:polyn, :vsn)}"
  end

  test "full_type/1 prefixes domain" do
    assert "com.test.user.created.v1" == Event.full_type("user.created.v1")
  end

  test "full_source/0 creates source with domain and source_root" do
    assert "com:test:user:backend" == Event.full_source()
  end

  test "full_source/1 raises if invalid name" do
    assert_raise(Polyn.ValidationException, fn ->
      assert "com:test:user:backend:orders" == Event.full_source("*orders*")
    end)
  end

  test "full_source/1 creates source with producer name appended" do
    assert "com:test:user:backend:orders" == Event.full_source("orders")
  end

  test "full_source/1 uses root if nil" do
    assert "com:test:user:backend" == Event.full_source(nil)
  end
end
