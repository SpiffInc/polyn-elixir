defmodule Polyn.PullConsumer do
  @moduledoc """
  Use `Polyn.PullConsumer` to connect and process messages from an existing [NATS Consumer](https://docs.nats.io/nats-concepts/jetstream/consumers)
  that was setup with [Polyn CLI](https://github.com/SpiffInc/polyn-cli). This module is a
  wrapper around `Jetstream.PullConsumer` that does schema validation with the received messages.
  A key difference that Polyn adds is that the `:consumer_name` will be taken care of for you
  by using the passed `type` and configured `:source_root`. You can pass `:source` to `start_link/3`
  to get a more specific `:consumer_name`. This type of Consumer is meant for simple use cases that
  don't involve concurrency or batching.

  ## Example

      defmodule MyApp.PullConsumer do
        use Polyn.PullConsumer

        def start_link(arg) do
          Polyn.PullConsumer.start_link(__MODULE__, arg,
            connection_name: :gnat,
            type: "user.created.v1")
        end

        @impl true
        def init(_arg) do
          {:ok, nil}
        end

        @impl true
        def handle_message(message, state) do
          # Do some processing with the message.
          {:ack, state}
        end
      end
  """

  use Jetstream.PullConsumer
  use Polyn.Tracing
  alias Polyn.Serializers.JSON

  defstruct [:module, :state, :store_name, :connection_name, :type, :source]

  @doc """
  Invoked when the server is started. `start_link/3` or `start/3` will block until it returns.

  `init_arg` is the argument term (second argument) passed to `start_link/3`.

  See `c:Connection.init/1` for more details.
  """
  @callback init(init_arg :: term) ::
              {:ok, state :: term()}
              | :ignore
              | {:stop, reason :: any}

  @doc """
  Invoked to synchronously process a message pulled by the consumer.
  Depending on the value it returns, the acknowledgement is or is not sent.
  Polyn will deserialize the message body into a `Polyn.Event` struct and use
  that as the first argument, followed by the original message, follwed by the state.

  ## ACK actions

  See `c:Jetstream.PullConsumer.handle_message/2` for available options

  ## Example

      def handle_message(event, _message, state) do
        IO.inspect(event)
        {:ack, state}
      end

  """
  @callback handle_message(
              event :: Polyn.Event.t(),
              message :: Jetstream.message(),
              state :: term()
            ) ::
              {ack_action, new_state}
            when ack_action: :ack | :nack | :term | :noreply, new_state: term()

  @typedoc """
  Options for starting a Polyn.PullConsumer

  * `:type` - Required. The event type to consume
  * `:connection_name` - Required. The Gnat connection identifier
  * `:source` - Optional. More specific name for the consumer to add to the `:source_root`
  * All other options will be assumed to be GenServer options
  """
  @type option ::
          {:type, binary()}
          | {:source, binary()}
          | {:connection_name, Gnat.t()}
          | GenServer.option()

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour Polyn.PullConsumer

      @spec child_spec(arg :: GenServer.options()) :: Supervisor.child_spec()
      def child_spec(arg) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [arg]}
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1
    end
  end

  defp new(module, opts) do
    opts = Keyword.put(opts, :module, module)
    struct(__MODULE__, opts)
  end

  @doc """
  Starts a pull consumer linked to the current process with the given function.

  This is often used to start the pull consumer as part of a supervision tree.

  Once the server is started, the `c:init/1` function of the given `module` is called with
  `init_arg` as its argument to initialize the server. To ensure a synchronized start-up procedure,
  this function does not return until `c:init/1` has returned.

  See `GenServer.start_link/3` for more details.

  ## Example

      {:ok, consumer} =
        Polyn.PullConsumer.start_link(ExamplePullConsumer, %{initial_arg: "foo"},
          connection_name: :gnat,
          type: "user.updated.v1",
          stream: "TEST_STREAM",
        )
  """
  @spec start_link(module(), init_arg :: term(), options :: [option()]) ::
          GenServer.on_start()
  def start_link(module, init_arg, opts \\ []) when is_atom(module) and is_list(opts) do
    pull_consumer(opts).start_link(
      __MODULE__,
      {new(module, opts), init_arg},
      opts
    )
  end

  @doc """
  Starts a `Jetstream.PullConsumer` process without links (outside of a supervision tree).

  See `start_link/3` for more information.
  """
  @spec start(module(), init_arg :: term(), options :: [option()]) ::
          GenServer.on_start()
  def start(module, init_arg, opts \\ []) when is_atom(module) and is_list(opts) do
    pull_consumer(opts).start(__MODULE__, {new(module, opts), init_arg}, opts)
  end

  @doc """
  Closes the pull consumer and stops underlying process.

  ## Example

      {:ok, consumer} =
        PullConsumer.start_link(ExamplePullConsumer, %{initial_arg: "foo"},
          connection_name: :gnat,
          type: "user.updated.v1",
          stream: "TEST_STREAM",
        )

      :ok = PullConsumer.close(consumer)

  """
  @spec close(consumer :: Jetstream.PullConsumer.consumer()) :: :ok
  def close(consumer) do
    pull_consumer().close(consumer)
  end

  @impl Jetstream.PullConsumer
  def init({%{module: module} = internal_state, init_arg}) do
    case module.init(init_arg) do
      {:ok, state} ->
        # Keep the `module` in the internal state so we can know
        # what functions to call
        internal_state = %{internal_state | state: state}

        {:ok, internal_state, connection_options(internal_state)}

      other ->
        other
    end
  end

  @impl Jetstream.PullConsumer
  def handle_message(message, %{module: module, state: state} = internal_state) do
    Polyn.Tracing.subscribe_span internal_state.type, message[:headers] do
      case JSON.deserialize(message.body, store_name: internal_state.store_name) do
        {:ok, event} ->
          Polyn.Tracing.span_attributes(
            conn: internal_state.connection_name,
            type: internal_state.type,
            event: event,
            payload: message.body
          )

          {response, state} = module.handle_message(event, message, state)

          {response, %{internal_state | state: state}}

        {:error, error} ->
          # If a validation error happens we want to tell NATS to stop sending the message
          # and that it won't be processed (ACKTERM) and will prevent us from raising the
          # same error over and over.
          Jetstream.ack_term(message)

          raise Polyn.ValidationException, error
      end
    end
  end

  defp connection_options(%{
         type: type,
         source: source,
         connection_name: connection_name
       }) do
    consumer_name = Polyn.Naming.consumer_name(type, source)
    stream = Polyn.Naming.lookup_stream_name!(connection_name, type)
    [connection_name: connection_name, stream_name: stream, consumer_name: consumer_name]
  end

  defp pull_consumer(opts \\ []) do
    if sandbox(opts), do: Polyn.Jetstream.MockPullConsumer, else: Polyn.Jetstream.PullConsumer
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
