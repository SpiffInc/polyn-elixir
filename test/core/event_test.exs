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
    assert source == "com:test:my_app"
  end

  test "type/2 creates an event with v1 by default" do
    assert "com.test.user.created.v1" == Event.type("user.created")
  end

  test "type/2 creates an event with a version" do
    assert "com.test.user.created.v2" == Event.type("user.created", version: 2)
  end

  test "dataschema/2 creates dataschema URI v1 by default" do
    assert "com:test:user:created:v1:schema:v1" ==
             Event.type("user.created") |> Event.dataschema()
  end

  test "dataschema/2 creates dataschema URI with other version" do
    assert "com:test:user:created:v1:schema:v2" ==
             Event.type("user.created") |> Event.dataschema(version: 2)
  end

  test "source/0 creates source with domain and source_root" do
    assert "com:test:my_app" == Event.source()
  end

  test "source/1 creates source with producer name appended" do
    assert "com:test:my_app:orders" == Event.source("orders")
  end

  test "with_bare_type/1 removes domain and version" do
    assert %Event{type: "user.created"} =
             Event.new(type: "com.test.user.created.v1") |> Event.with_bare_type()
  end
end
