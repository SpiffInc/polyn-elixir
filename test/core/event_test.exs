defmodule Polyn.EventTest do
  use ExUnit.Case, async: true

  alias Polyn.Event

  test "type/1 creates an event" do
    assert "com.test.user.created" = Event.type("user.created")
  end

  test "type/2 creates an event with a version" do
    assert "com.test.user.created.v2" = Event.type("user.created", version: 2)
  end
end
