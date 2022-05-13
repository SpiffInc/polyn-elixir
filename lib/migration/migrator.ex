defmodule Polyn.Migrator do
  # Different system components
  # Different programming languages
  # All making configuration changes to central data system
  # Doing it in a centralized place would create a productivity bottleneck
  # There needs to be a complete record of all migrations from all components
  # Where are those stitched together?
  # Need a common DSL language to represent instructions so that each programming
  # language can convert them to something that can execute
  # Can put them all in a stream call "migrations"

  # Migrations need to run on application start
  # They need to not run if they've already run (A delete could be destructive)
  # They need to stay in order
  # Each migration command can be a CloudEvent with a JSON Schema

  require Logger
  alias Jetstream.API.Stream

  @migration_stream "POLYN_MIGRATIONS"
  @migration_subject "POLYN_MIGRATIONS.all"

  defmodule State do
    # Holds the state of the migration as we move through migration steps
    @moduledoc false

    @type t :: %Module{}

    defstruct [
      :config_service_auth_token,
      :migration_stream_info,
      already_run_migrations: [],
      production_migrations: [],
      application_migrations: []
    ]

    def new(opts \\ []) do
      struct!(__MODULE__, opts)
    end
  end

  defmodule MigrationException do
    @moduledoc false
    defexception [:message]
  end

  def run(config_service_auth_token) do
    init_state(config_service_auth_token: config_service_auth_token)
    |> fetch_migration_stream_info()
    |> create_migration_stream()
    |> fetch_production_migrations()
    |> fetch_already_run_migrations()
    |> read_application_migrations()
  end

  defp init_state(opts) do
    struct!(State, opts)
  end

  defp create_migration_stream(%{migration_stream_info: nil} = state) do
    stream =
      struct!(Stream, %{
        name: @migration_stream,
        subjects: [@migration_subject],
        discard: :new,
        # TODO: Update based on cluster info
        num_replicas: 1
      })

    case Stream.create(connection_name(), stream) do
      {:error, reason} ->
        raise MigrationException, inspect(reason)

      {:ok, info} ->
        Map.put(state, :migration_stream_info, info)
    end
  end

  defp create_migration_stream(state), do: state

  defp fetch_migration_stream_info(state) do
    case Stream.info(connection_name(), @migration_stream) do
      {:ok, info} -> Map.put(state, :migration_stream_info, info)
      _ -> state
    end
  end

  # Migrations that have been already been run in whatever server
  # we are connected to, could be production, test, or local
  defp fetch_already_run_migrations(state), do: state

  # Migrations that have been run in the production server
  defp fetch_production_migrations(%{config_service_auth_token: _token} = state) do
    # HTTP.get(Application.fetch_env!(:polyn, :config_service_url))
    # |> Polyn.Serializers.JSON.deserialize()

    state
  end

  # Migrations that the application using Polyn owns
  defp read_application_migrations(state) do
    local_migration_files()
    |> compile_migration_files()
    |> execute_migration_modules(state)
  end

  defp local_migration_files do
    case file().ls(migrations_dir()) do
      {:ok, files} ->
        files
        |> Enum.filter(&is_elixir_script?/1)
        |> Enum.sort_by(&extract_migration_timestamp/1)

      {:error, _reason} ->
        Logger.info("No migrations found at #{migrations_dir()}")
        []
    end
  end

  defp is_elixir_script?(file_name) do
    String.ends_with?(file_name, ".exs")
  end

  defp extract_migration_timestamp(file_name) do
    [timestamp | _] = String.split(file_name, "_")
    String.to_integer(timestamp)
  end

  defp compile_migration_files(files) do
    Enum.map(files, fn file_name ->
      [{module, _content}] = code().compile_file(Path.join(migrations_dir(), file_name))
      module
    end)
  end

  defp execute_migration_modules(modules, state) do
    Enum.reduce(modules, state, fn module, acc ->
      module.change(acc)
    end)
  end

  defp connection_name do
    connection_config().name
  end

  defp connection_config do
    Application.fetch_env!(:polyn, :nats)
  end

  defp migrations_dir do
    Path.join(file().cwd!(), "/priv/polyn/migrations")
  end

  defp file do
    Application.get_env(:polyn, :file, File)
  end

  defp code do
    Application.get_env(:polyn, :code, Code)
  end
end
