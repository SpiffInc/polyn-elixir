defmodule Polyn.SchemaStore do
  @moduledoc """
  A SchemaStore for loading and accessing schemas from the NATS server that were
  created via Polyn CLI.

  You will need this running, likely in your application supervision tree, in order for
  Polyn to access schemas

  ## Examples

      ```elixir
      children = [
        {Polyn.SchemaStore, connection_name: :connection_name_or_pid}
      ]

      opts = [strategy: :one_for_one, name: MySupervisor]
      Supervisor.start_link(children, opts)
      ```
  """

  use GenServer

  alias Jetstream.API.KV

  @store_name "POLYN_SCHEMAS"

  @type option :: {:connection_name, Gnat.t()} | GenServer.option()

  @doc """
  Start a new SchemaStore process

  ## Examples

      iex>Polyn.SchemaStore.start_link(connection_name: :gnat)
      :ok
  """
  @spec start_link(opts :: [option()]) :: GenServer.on_start()
  def start_link(opts) do
    {store_args, server_opts} = Keyword.split(opts, [:schemas, :store_name, :connection_name])
    # For applications and application testing there should only be one SchemaStore running.
    # For testing the library there could be multiple
    process_name = Keyword.get(store_args, :store_name) |> process_name()
    server_opts = Keyword.put_new(server_opts, :name, process_name)
    GenServer.start_link(__MODULE__, store_args, server_opts)
  end

  # Get a process name for a given store name
  @doc false
  def process_name(nil), do: __MODULE__
  def process_name(store_name) when is_binary(store_name), do: String.to_atom(store_name)
  def process_name(store_name) when is_atom(store_name), do: store_name

  @doc false
  @spec get_schemas(pid()) :: map()
  def get_schemas(pid) do
    GenServer.call(pid, :get_schemas)
  end

  # Persist a schema. In prod/dev schemas should have already been persisted via
  # the Polyn CLI.
  @doc false
  @spec save(pid :: pid(), type :: binary(), schema :: map()) :: :ok
  def save(pid, type, schema) when is_map(schema) do
    is_json_schema?(schema)
    GenServer.call(pid, {:save, type, encode(schema)})
  end

  defp is_json_schema?(schema) do
    ExJsonSchema.Schema.resolve(schema)
  rescue
    ExJsonSchema.Schema.InvalidSchemaError ->
      reraise Polyn.SchemaException,
              [message: "Schemas must be valid JSONSchema documents, got #{inspect(schema)}"],
              __STACKTRACE__
  end

  defp encode(schema) do
    case Jason.encode(schema) do
      {:ok, encoded} -> encoded
      {:error, reason} -> raise Polyn.SchemaException, inspect(reason)
    end
  end

  # Remove a schema
  @doc false
  @spec delete(pid :: pid(), type :: binary()) :: :ok
  def delete(pid, type) do
    GenServer.call(pid, {:delete, type})
  end

  # Get the schema for an event
  @doc false
  @spec get(pid :: pid(), type :: binary()) :: nil | map()
  def get(pid, type) do
    case GenServer.call(pid, {:get, type}) do
      nil ->
        nil

      schema ->
        Jason.decode!(schema)
    end
  end

  # Create the schema store if it doesn't exist already. In prod/dev the the store
  # creation should have already been done via the Polyn CLI
  @doc false
  @spec create_store(conn :: Gnat.t()) :: :ok
  @spec create_store(conn :: Gnat.t(), opts :: keyword()) :: :ok
  def create_store(conn, opts \\ []) do
    result =
      KV.create_bucket(conn, store_name(opts),
        description: "Contains Schemas for all events on the server"
      )

    case result do
      {:ok, _info} -> :ok
      # If some other client created the store first, with a slightly different
      # description or config we'll just use the existing one
      {:error, %{"err_code" => 10_058}} -> :ok
      {:error, reason} -> raise Polyn.SchemaException, inspect(reason)
    end
  end

  # Delete the schema store. Useful for test
  @doc false
  @spec delete_store(conn :: Gnat.t()) :: :ok
  @spec delete_store(conn :: Gnat.t(), opts :: keyword()) :: :ok
  def delete_store(conn, opts \\ []) do
    KV.delete_bucket(conn, store_name(opts))
  end

  # Get a configured store name or the default
  @doc false
  def store_name(opts \\ []) do
    Keyword.get(opts, :name, @store_name)
  end

  @impl GenServer
  def init(init_args) do
    store_name = Keyword.get(init_args, :store_name, @store_name)
    conn = Keyword.fetch!(init_args, :connection_name)
    preloaded_schemas = Keyword.get(init_args, :schemas)

    schemas = preloaded_schemas || load_schemas(conn, store_name)

    {:ok, %{conn: conn, store_name: store_name, schemas: schemas}}
  end

  defp load_schemas(conn, store_name) do
    case KV.contents(conn, store_name) do
      {:ok, schemas} ->
        schemas

      {:error, reason} ->
        raise Polyn.SchemaException, inspect(reason)
    end
  end

  @impl GenServer
  def handle_call(:get_schemas, _from, state) do
    {:reply, state.schemas, state}
  end

  def handle_call({:save, type, schema}, _from, state) do
    schemas = Map.put(state.schemas, type, schema)
    {:reply, :ok, %{state | schemas: schemas}}
  end

  def handle_call({:get, type}, _from, state) do
    {:reply, Map.get(state.schemas, type), state}
  end

  def handle_call({:delete, type}, _from, state) do
    schemas = Map.delete(state.schemas, type)
    {:reply, :ok, %{state | schemas: schemas}}
  end
end
