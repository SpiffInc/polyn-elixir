defmodule Polyn.MigrationTest do
  use ExUnit.Case, async: true

  import Polyn.Migration
  alias Polyn.Migrator.State

  test "create_stream/2 adds event data to migrator state" do
    state =
      State.new()
      |> create_stream(state, name: stream_name, subjects: ["foo"])

    assert [create_stream_event] = state.application_migrations
  end
end
