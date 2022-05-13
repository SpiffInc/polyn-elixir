defmodule Polyn.MigratorTest do
  use ExUnit.Case, async: true

  alias Polyn.CodeMock
  alias Polyn.FileMock
  alias Polyn.Migrator
  alias Jetstream.API.Stream

  import Mox

  setup :verify_on_exit!

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

  test "adds a migration to create a new stream" do
    defmodule ExampleCreateStream do
      import Polyn.Migration

      def change(migration_state) do
        create_stream(name: "test_stream", subjects: ["test_subject"])
      end
    end

    expect_cwd!("my_app", 2)
    expect_ls("my_app", ["1234_create_stream.exs"])
    expect_compile_file("my_app", "1234_create_stream.exs", [ExampleCreateStream])
    Migrator.run("my_auth_token")
  end

  test "local migrations ignore non .exs files" do
  end

  test "local migrations in correct order" do
  end

  test "logs when no local migrations found" do
  end

  defp expect_ls(cwd, files) do
    path = "#{cwd}/priv/polyn/migrations"
    expect(FileMock, :ls, fn ^path -> {:ok, files} end)
  end

  defp expect_error_ls(cwd) do
    path = "#{cwd}/priv/polyn/migrations"
    expect(FileMock, :ls, fn ^path -> {:error, :enoent} end)
  end

  defp expect_cwd!(cwd, n \\ 1) do
    expect(FileMock, :cwd!, n, fn -> cwd end)
  end

  defp expect_compile_file(cwd, file, modules) do
    path = "#{cwd}/priv/polyn/migrations/#{file}"

    expect(CodeMock, :compile_file, fn ^path ->
      Enum.map(modules, fn module -> {module, "foo"} end)
    end)
  end

  defp connection_name do
    connection_config().name
  end

  defp connection_config do
    Application.fetch_env!(:polyn, :nats)
  end
end
