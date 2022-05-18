defmodule Polyn.MigratorTest do
  use ExUnit.Case, async: true

  alias Jetstream.API.Stream
  alias Polyn.Migrator
  import ExUnit.CaptureLog

  import Mox
  alias Polyn.FileMock

  @moduletag :tmp_dir

  @migration_stream "POLYN_MIGRATIONS"
  @migration_subject "POLYN_MIGRATIONS.all"
  @migration_events_dir "priv/migration_events"

  setup do
    on_exit(fn ->
      Stream.delete(connection_name(), @migration_stream)
    end)
  end

  test "creates migration stream if not there", %{tmp_dir: tmp_dir} do
    Stream.delete(connection_name(), @migration_stream)
    Migrator.run(["foo", tmp_dir])
    assert {:ok, info} = Stream.info(connection_name(), @migration_stream)
    assert info.config.name == @migration_stream
  end

  test "ignores migration stream if already existing", %{tmp_dir: tmp_dir} do
    {:ok, _stream} =
      Stream.create(connection_name(), %Stream{
        name: @migration_stream,
        subjects: [@migration_subject]
      })

    Migrator.run(["foo", tmp_dir])
    assert {:ok, info} = Stream.info(connection_name(), @migration_stream)
    assert info.config.name == @migration_stream
  end

  test "adds a migration to create a new stream", %{tmp_dir: tmp_dir} do
    migration = """
    defmodule ExampleCreateStream do
      import Polyn.Migration

      def change do
        create_stream(name: "test_stream", subjects: ["test_subject"])
      end
    end
    """

    add_migration_file(tmp_dir, "1234_create_stream.exs", migration)

    expect_schema_read(
      "polyn.stream.create.v1",
      "polyn.stream.create.v1.schema.v1.json"
    )

    Migrator.run(["my_auth_token", tmp_dir])

    assert {:ok, %{data: data}} =
             Stream.get_message(connection_name(), @migration_stream, %{
               last_by_subj: @migration_subject
             })

    data = Jason.decode!(data)
    assert data["type"] == "com.test.polyn.stream.create.v1"
    assert data["data"]["name"] == "test_stream"
    assert data["data"]["subjects"] == ["test_subject"]

    assert {:ok, info} = Stream.info(connection_name(), "test_stream")
    assert info.config.name == "test_stream"
    Stream.delete(connection_name(), "test_stream")
  end

  test "local migrations ignore non .exs files" do
  end

  test "local migrations in correct order" do
  end

  test "logs when no local migrations found", %{tmp_dir: tmp_dir} do
    assert capture_log(fn ->
             Migrator.run(["my_auth_token", tmp_dir])
           end) =~ "No migrations found at #{tmp_dir}"
  end

  defp add_migration_file(dir, file_name, contents) do
    File.write!(Path.join(dir, file_name), contents)
  end

  defp expect_schema_read(event, schema_file) do
    path = Application.app_dir(:polyn, [@migration_events_dir, event, schema_file])

    expect(FileMock, :read, fn ^path ->
      {:ok, File.read!(path)}
    end)
  end

  defp connection_name do
    connection_config().name
  end

  defp connection_config do
    Application.fetch_env!(:polyn, :nats)
  end
end
