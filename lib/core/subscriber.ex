defmodule Polyn.Subscriber do
  @moduledoc """
  A GenServer wrapper to use when working with vanilla NATS subscriptions outside of JetStream.
  This process will hang around and listen for messages to come in and then trigger a `c:handle_message/3` callback

  ```elixir
  defmodule MySubscriber do
    use Polyn.Subscriber

    def start_link(init_args) do
      Polyn.Subscriber.start_link(__MODULE__, init_args,
        connection_name: :gnat,
        event: "user.created.v1")
    end

    def init(_arg) do
      {:ok, nil}
    end

    def handle_message(event, message, state) do
      # Do something cool with the event
      {:noreply, state}
    end
  end
  ```
  """

  use GenServer
  use Polyn.Tracing

  alias Polyn.Event
  alias Polyn.Serializers.JSON

  @type start_options :: GenServer.option() | {:connection_name, Gnat.t()} | {:event, binary()}

  @doc """
  Called when the subscribed event is published. Return the same values as you would for a
  `c:Genserver.handle_info/2` callback
  """
  @callback handle_message(event :: Event.t(), msg :: Gnat.message(), state :: any()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout() | :hibernate | {:continue, term()}}
              | {:stop, reason :: term(), new_state}
            when new_state: term()

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour Polyn.Subscriber
      use GenServer
    end
  end

  @doc """

  ## Options

  * `:connection_name` - Required, The pid or name of Gnat connection
  * `:event` - Required, The name of the event to subscribe to
  * `:queue_group` - a string that identifies which queue group you want to join

  Any other passed options will be assumed to be GenServer options
  """
  @spec start_link(module(), init_args :: any(), options :: [start_options()]) ::
          GenServer.on_start()
  def start_link(module, init_args, options) do
    {sub_opts, genserver_opts} =
      Keyword.split(options, [:connection_name, :event, :queue_group, :store_name, :sandbox])

    GenServer.start_link(__MODULE__, {module, init_args, sub_opts}, genserver_opts)
  end

  @impl true
  def init({module, init_args, opts}) do
    {opts, sub_opts} = Keyword.split(opts, [:connection_name, :event, :store_name, :sandbox])

    conn = Keyword.fetch!(opts, :connection_name)
    event = Keyword.fetch!(opts, :event)

    case nats(opts).sub(conn, self(), event, sub_opts) do
      {:ok, subscription} ->
        on_subscribe_success(subscription, module, init_args, opts)

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp on_subscribe_success(subscription, module, init_args, opts) do
    case module.init(init_args) do
      {:ok, state} ->
        {:ok, initial_state(state, module, subscription, opts)}

      {:ok, state, other} ->
        {:ok, initial_state(state, module, subscription, opts), other}

      other ->
        other
    end
  end

  defp initial_state(state, module, subscription, opts) do
    %{
      state: state,
      module: module,
      subscription: subscription,
      opts: opts
    }
  end

  @impl true
  def handle_info({:msg, %{body: body} = msg}, internal_state) do
    Polyn.Tracing.subscribe_span msg.topic, msg[:headers] do
      event =
        JSON.deserialize!(
          body,
          internal_state.opts
        )

      Polyn.Tracing.span_attributes(
        conn: internal_state.opts[:connection_name],
        type: internal_state.opts[:event],
        event: event,
        payload: body
      )

      case internal_state.module.handle_message(event, msg, internal_state.state) do
        {:noreply, state} ->
          {:noreply, Map.put(internal_state, :state, state)}

        {:noreply, state, other} ->
          {:noreply, Map.put(internal_state, :state, state), other}

        other ->
          other
      end
    end
  end

  defp nats(opts) do
    if sandbox(opts), do: Polyn.MockNats, else: Gnat
  end

  # If Application config dictates "sandbox mode" we prioritize that
  # otherwise we defer to a passed in option
  defp sandbox(opts) do
    case Application.get_env(:polyn, :sandbox) do
      nil -> Keyword.get(opts, :sandbox, false)
      other -> other
    end
  end
end
