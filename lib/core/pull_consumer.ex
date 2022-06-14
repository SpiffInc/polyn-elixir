defmodule Polyn.PullConsumer do
  @moduledoc """
  Use `Polyn.PullConsumer` to connect and process messages from an existing [NATS Consumer](https://docs.nats.io/nats-concepts/jetstream/consumers)
  that was setup with Polyn CLI. This module is a wrapper around `Jetstream.PullConsumer` that
  does schema validation with the received messages. This type of Consumer is meant for simple
  use cases that don't involve concurrency or batching.
  """

  use Jetstream.PullConsumer
  alias Polyn.Serializers.JSON

  @doc """
  Invoked when the server is started. `start_link/3` or `start/3` will block until it returns.

  `init_arg` is the argument term (second argument) passed to `start_link/3`.

  See `c:Connection.init/1` for more details.
  """
  @callback init(init_arg :: term) ::
              {:ok, state :: term(), Jetstream.PullConsumer.connection_options()}
              | :ignore
              | {:stop, reason :: any}

  @doc """
  Invoked to synchronously process a message pulled by the consumer.
  Depending on the value it returns, the acknowledgement is or is not sent.
  Polyn will deserialize the message body into an `Polyn.Event` struct and use
  that as the first argument, followed by the original message, follwed by the state.

  ## ACK actions

  See `Jetstream.PullConsumer.handle_message/2` for available options

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

  @doc """
  Starts a pull consumer linked to the current process with the given function.

  This is often used to start the pull consumer as part of a supervision tree.

  Once the server is started, the `c:init/1` function of the given `module` is called with
  `init_arg` as its argument to initialize the server. To ensure a synchronized start-up procedure,
  this function does not return until `c:init/1` has returned.

  See `GenServer.start_link/3` for more details.
  """
  @spec start_link(module(), init_arg :: term(), options :: GenServer.options()) ::
          GenServer.on_start()
  def start_link(module, init_arg, options \\ []) when is_atom(module) and is_list(options) do
    Jetstream.PullConsumer.start_link(
      __MODULE__,
      {initial_state(module, options), init_arg},
      options
    )
  end

  @doc """
  Starts a `Jetstream.PullConsumer` process without links (outside of a supervision tree).

  See `start_link/3` for more information.
  """
  @spec start(module(), init_arg :: term(), options :: GenServer.options()) ::
          GenServer.on_start()
  def start(module, init_arg, options \\ []) when is_atom(module) and is_list(options) do
    Jetstream.PullConsumer.start(__MODULE__, {initial_state(module, options), init_arg}, options)
  end

  @doc """
  Closes the pull consumer and stops underlying process.

  ## Example

      {:ok, consumer} =
        PullConsumer.start_link(ExamplePullConsumer,
          connection_name: :gnat,
          stream_name: "TEST_STREAM",
          consumer_name: "TEST_CONSUMER"
        )

      :ok = PullConsumer.close(consumer)

  """
  @spec close(consumer :: Jetstream.PullConsumer.consumer()) :: :ok
  def close(consumer) do
    Jetstream.PullConsumer.close(consumer)
  end

  @impl Jetstream.PullConsumer
  def init({%{module: module} = internal_state, init_arg}) do
    case module.init(init_arg) do
      {:ok, state, connection_options} ->
        # Keep the `module` in the internal state so we can know
        # what functions to call
        internal_state = %{internal_state | state: state, connection_options: connection_options}

        {:ok, internal_state, connection_options}

      other ->
        other
    end
  end

  @impl Jetstream.PullConsumer
  def handle_message(message, %{module: module, state: state} = internal_state) do
    conn = Keyword.fetch!(internal_state.connection_options, :connection_name)

    case JSON.deserialize(message.body, conn, store_name: internal_state.store_name) do
      {:ok, event} ->
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

  defp initial_state(module, opts) do
    %{module: module, state: nil, store_name: store_name(opts), connection_options: nil}
  end

  defp store_name(opts) do
    Keyword.get(opts, :store_name, Polyn.SchemaStore.store_name())
  end
end
