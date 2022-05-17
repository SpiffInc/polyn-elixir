defmodule Polyn.MigrationTest do
  use ExUnit.Case, async: true

  import Polyn.Migration
  alias Polyn.Event
  alias Polyn.Migrator.{LocalRunner, State}

  setup do
    pid =
      start_supervised!(
        {LocalRunner, State.new(running_migration_id: "1234", running_migration_command_num: 0)}
      )

    Process.put(:polyn_local_migration_runner, pid)

    %{runner: pid}
  end

  test "create_stream/2 adds event data to migrator state", %{runner: pid} do
    create_stream(name: "test_stream", subjects: ["foo"])
    assert %{application_migrations: [migration]} = LocalRunner.get_state(pid)

    assert %Event{
             id: "1234.1",
             type: "com.test.polyn.stream.create.v1",
             specversion: "1.0.1",
             source: "com:test:my_app",
             dataschema: "polyn:stream:create:v1:schema:v1",
             data: %{name: "test_stream", subjects: ["foo"]}
           } = migration
  end

  test "create_stream/2 called multiple works", %{runner: pid} do
    create_stream(name: "test_stream", subjects: ["foo"])
    create_stream(name: "test_stream2", subjects: ["bar"])

    assert %{application_migrations: [migration1, migration2]} = LocalRunner.get_state(pid)

    assert %Event{
             id: "1234.1",
             type: "com.test.polyn.stream.create.v1",
             specversion: "1.0.1",
             source: "com:test:my_app",
             dataschema: "polyn:stream:create:v1:schema:v1",
             data: %{name: "test_stream", subjects: ["foo"]}
           } = migration1

    assert %Event{
             id: "1234.2",
             type: "com.test.polyn.stream.create.v1",
             specversion: "1.0.1",
             source: "com:test:my_app",
             dataschema: "polyn:stream:create:v1:schema:v1",
             data: %{name: "test_stream2", subjects: ["bar"]}
           } = migration2
  end
end
