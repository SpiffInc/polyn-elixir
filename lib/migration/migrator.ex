defmodule Polyn.Migrator do
  @moduledoc false
  require Logger
  alias Jetstream.API.Stream
  alias Polyn.Serializers.JSON

  @migration_stream "POLYN_MIGRATIONS"
  @migration_subject "POLYN_MIGRATIONS.all"

  defmodule State do
    # Holds the state of the migration as we move through migration steps
    @moduledoc false

    @typedoc """
    * `:running_migration_id` - The timestamp/id of the migration file being run. Taken from the beginning of the file name
    * `:running_migration_command_num` - The number of the command being run in the migration module
    * `:config_service_auth_token` - The auth token to access a production API endpoint containing production migration events
    * `:already_run_migrations` - Migrations we've determined have already been executed on the server
    * `:production_migrations` - Migrations that have been run on the production server already
    * `:application_migrations` - Migrations that live locally in the codebase
    """

    @type t :: %__MODULE__{
            running_migration_id: non_neg_integer() | nil,
            running_migration_command_num: non_neg_integer() | nil,
            config_service_auth_token: binary(),
            migration_stream_info: Stream.info() | nil,
            already_run_migrations: list(Polyn.Event.t()),
            production_migrations: list(Polyn.Event.t()),
            application_migrations: list(Polyn.Event.t())
          }

    defstruct [
      :running_migration_id,
      :running_migration_command_num,
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

  defmodule LocalRunner do
    # A way to update the migration state without exposing it to
    # developers creating migration files. This will allow Migration
    # functions to update the state without developers needing to be
    # aware of it.
    @moduledoc false
    use Agent

    def start_link(state) do
      Agent.start_link(fn -> state end)
    end

    def stop(pid) do
      Agent.stop(pid)
    end

    @doc "Add a new migration event to the application migrations state"
    def add_application_migration(pid, event) do
      Agent.update(pid, fn state ->
        migrations = Enum.concat(state.application_migrations, [event])
        Map.put(state, :application_migrations, migrations)
      end)
    end

    @doc "Update the state to know the id of the migration running"
    def update_running_migration_id(pid, id) do
      Agent.update(pid, fn state ->
        Map.put(state, :running_migration_id, id)
      end)
    end

    @doc "Update the state to know the number of the command running in the migration"
    def update_running_migration_command_num(pid, num) do
      Agent.update(pid, fn state ->
        Map.put(state, :running_migration_command_num, num)
      end)
    end

    def get_running_migration_id(pid) do
      get_state(pid).running_migration_id
    end

    def get_running_migration_command_num(pid) do
      get_state(pid).running_migration_command_num
    end

    def get_state(pid) do
      Agent.get(pid, fn state -> state end)
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
    |> run_migrations()
  end

  defp init_state(opts) do
    struct!(State, opts)
  end

  # We'll keep all migrations on a JetStream Stream so that we can
  # keep them in order and mesh them together from all the different
  # system components
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
        |> Enum.sort_by(&extract_migration_id/1)

      {:error, _reason} ->
        Logger.info("No migrations found at #{migrations_dir()}")
        []
    end
  end

  defp is_elixir_script?(file_name) do
    String.ends_with?(file_name, ".exs")
  end

  defp extract_migration_id(file_name) do
    [id | _] = String.split(file_name, "_")
    String.to_integer(id)
  end

  defp compile_migration_files(files) do
    Enum.map(files, fn file_name ->
      id = extract_migration_id(file_name)
      [{module, _content}] = code().compile_file(Path.join(migrations_dir(), file_name))
      {module, id}
    end)
  end

  # Get the events produced by each migration file
  defp execute_migration_modules(modules, state) do
    {:ok, pid} = LocalRunner.start_link(state)
    Process.put(:polyn_local_migration_runner, pid)

    Enum.each(modules, fn {module, id} ->
      LocalRunner.update_running_migration_id(pid, id)
      LocalRunner.update_running_migration_command_num(pid, 0)
      module.change()
    end)

    LocalRunner.update_running_migration_id(pid, nil)
    LocalRunner.update_running_migration_command_num(pid, nil)
    state = LocalRunner.get_state(pid)

    LocalRunner.stop(pid)
    state
  end

  defp run_migrations(state) do
    Enum.each(state.application_migrations, fn event ->
      serialized_event = JSON.serialize(event)
      Gnat.pub(connection_name(), @migration_subject, serialized_event)
      execute_migration_event(Polyn.Event.with_bare_type(event))
    end)
  end

  defp execute_migration_event(%{type: "polyn.stream.create"} = event) do
    stream = struct(Stream, event.data)
    Stream.create(connection_name, stream)
  end

  defp connection_name do
    connection_config().name
  end

  defp connection_config do
    Application.fetch_env!(:polyn, :nats)
  end

  def migrations_dir do
    Path.join(file().cwd!(), "/priv/polyn/migrations")
  end

  defp file do
    Application.get_env(:polyn, :file, File)
  end

  defp code do
    Application.get_env(:polyn, :code, Code)
  end
end
