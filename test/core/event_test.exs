defmodule Polyn.EventTest do
  use ExUnit.Case, async: true

  alias Polyn.Event

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
end
