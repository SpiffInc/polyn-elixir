defmodule Polyn.Migration do
  @moduledoc """
  Functions for making changes to a NATS server
  """
  @callback change() :: nil

  alias Jetstream.API.Stream
  alias Polyn.Event
  alias Polyn.Migrator.LocalRunner
  alias Polyn.Naming

  @spec create_stream(stream_options :: keyword()) :: :ok
  def create_stream(options) when is_list(options) do
    Enum.into(options, %{})
    |> create_stream()
  end

  @spec create_stream(stream_options :: map()) :: :ok
  def create_stream(options) when is_map(options) do
    # Pick up defaults from Stream structs to help make event valid
    data = struct(Stream, options) |> Map.from_struct()
    command_num = LocalRunner.get_running_migration_command_num(runner()) + 1
    migration_id = LocalRunner.get_running_migration_id(runner())
    type = Event.type("polyn.stream.create", version: 1)

    event =
      Event.new(
        id: event_id(migration_id, command_num),
        # It's ok if the event version changes because old
        # migrations won't be run again in production if the IDs
        # are the same
        type: type,
        specversion: "1.0.1",
        # We don't want the consuming application's domain in the polyn
        # dataschemas since they will be the same in every app
        dataschema: Naming.trim_domain_prefix(type) |> Event.dataschema(),
        source: Event.source(),
        data: data
      )

    LocalRunner.add_application_migration(runner(), event)
    LocalRunner.update_running_migration_command_num(runner(), command_num)
  end

  # @spec delete_stream(stream_name :: binary()) :: :ok | {:error, any()}
  # def delete_stream(stream_name) do
  # end

  # The id of the event will be used to keep migrations in order.
  # The migration_id will be an integer that comes from the migration
  # file name and will be time-based. The command_num will be used to
  # keep track of the order the commands were executed within a single
  # migration file. If two applications happen to generate a migration at
  # the exact same time, the `source` will be used as a 2nd sorting factor
  defp event_id(migration_id, command_num) do
    "#{migration_id}.#{command_num}"
  end

  defp runner do
    Process.get(:polyn_local_migration_runner)
  end
end
