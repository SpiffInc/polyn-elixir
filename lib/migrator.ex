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

  alias Jetstream.API.Stream

  @migration_stream "POLYN_MIGRATIONS"
  @migration_subject "POLYN_MIGRATIONS.all"

  defmodule State do
    defstruct [:config_service_auth_token, :migration_stream_info]
  end

  defmodule MigrationException do
    defexception [:message]
  end

  def run(config_service_auth_token) do
    init_state(config_service_auth_token: config_service_auth_token)
    |> fetch_migration_stream_info()
    |> create_migration_stream()
    |> fetch_previous_migrations()
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

  defp fetch_previous_migrations(%{config_service_auth_token: _token} = state) do
    # HTTP.get(Application.fetch_env!(:polyn, :config_service_url))
    # |> Polyn.Serializers.JSON.deserialize()

    state
  end

  defp connection_name do
    connection_config().name
  end

  defp connection_config do
    Application.fetch_env!(:polyn, :nats)
  end
end
