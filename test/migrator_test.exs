defmodule Polyn.MigratorTest do
  use ExUnit.Case, async: true

  alias Polyn.Migrator
  alias Jetstream.API.Stream

  @migration_stream "POLYN_MIGRATIONS"
  @migration_subject "POLYN_MIGRATIONS.all"

  test "creates migration stream if not there" do
    Stream.delete(connection_name(), @migration_stream)
    Migrator.run("foo")
    assert {:ok, info} = Stream.info(connection_name(), @migration_stream)
    assert info.config.name == @migration_stream
    Stream.delete(connection_name(), @migration_stream)
  end

  test "ignores migration stream if already existing" do
    {:ok, _stream} =
      Stream.create(connection_name(), %Stream{
        name: @migration_stream,
        subjects: [@migration_subject]
      })

    Migrator.run("foo")
    assert {:ok, info} = Stream.info(connection_name(), @migration_stream)
    assert info.config.name == @migration_stream
    Stream.delete(connection_name(), @migration_stream)
  end

  defp connection_name do
    connection_config().name
  end

  defp connection_config do
    Application.fetch_env!(:polyn, :nats)
  end
end
