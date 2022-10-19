with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule OffBroadway.Polyn.Producer do
    @moduledoc """
    A [Broadway](https://hexdocs.pm/broadway/Broadway.html) Producer for Polyn.

    The word `Producer` here is confusing because the word is overloaded.
    In this module `Producer` refers to [GenStage](https://hexdocs.pm/gen_stage/GenStage.html) data
    pipelines where a `:producer` is the stage that receives demand for data and sends it to a `:consumer`.
    This module doesn't "produce" new events that get added to the NATS server for other services to consume.
    Rather it consumes existing events from a NATS Stream and passes them to GenStage `:consumer` modules
    in one application.

    ## Usage

    This module wraps `OffBroadway.Jetstream.Producer` and will validate that any messages coming through
    are valid events and conform to the schema for the event. Use the `OffBroadway.Jetstream.Producer` documentation
    to learn how to use it. One difference is that you will use `OffBroadway.Polyn.Producer`
    in your `:module` configuration instead of the Jetstream one. Invalid messages will send an ACKTERM
    to the NATS server so that they aren't sent again. They will be marked as `failed` and removed from the pipeline.
    Valid messages that come in a batch with an invalid message will send a NACK response before an error
    is raised so that the NATS server will know they were received but need to be sent again

    Another key difference that Polyn adds is that the `:consumer_name` will be taken care of for you
    by using the passed `type` and configured `:source_root`. You can pass in a `:source` to `:module`
    to get a more specific `:consumer_name`.

    ## Example

    ```elixir
    defmodule MyBroadway do
      use Broadway

      def start_link(_opts) do
        Broadway.start_link(
          __MODULE__,
          name: MyBroadway,
          producer: [
            module: {
              OffBroadway.Polyn.Producer,
              connection_name: :gnat,
              type: "user.created.v1"
            },
            concurrency: 10
          ],
          processors: [
            default: [concurrency: 10]
          ],
          batchers: [
            example: [
              concurrency: 5,
              batch_size: 10,
              batch_timeout: 2_000
            ]
          ]
        )
      end

      def handle_message(_processor_name, message, _context) do
        message
        |> Message.update_data(&process_data/1)
        |> Message.put_batcher(:example)
      end
    end
    ```
    """
    use GenStage
    use Polyn.Tracing

    alias Broadway.{Message, Producer}
    alias Polyn.SchemaStore
    alias Polyn.Serializers.JSON

    @behaviour Producer

    @impl Producer
    def prepare_for_start(module, broadway_opts) do
      opts = broadway_opts[:producer][:module] |> elem(1)
      producer(opts).prepare_for_start(module, broadway_opts)
    end

    @impl Producer
    def prepare_for_draining(state) do
      state.producer.prepare_for_draining(state)
    end

    @impl GenStage
    def handle_info(any, state) do
      response = state.producer.handle_info(any, state)

      case Tuple.to_list(response) do
        [:noreply, messages, state | rest] -> process_messages(messages, state, rest)
        _other -> response
      end
    end

    @impl GenStage
    def init(opts) do
      opts = add_consumer_and_stream_name(opts)
      producer = producer(opts)

      [:producer, state | rest] = producer.init(opts) |> Tuple.to_list()

      state =
        Map.put(state, :store_name, store_name(opts))
        |> Map.put(:producer, producer)
        |> Map.put(:acknowledger, acknowledger(opts))
        |> Map.put(:type, opts[:type])

      [:producer, state | rest] |> List.to_tuple()
    end

    @impl GenStage
    def handle_demand(incoming_demand, state) do
      response = state.producer.handle_demand(incoming_demand, state)

      case Tuple.to_list(response) do
        [:noreply, messages, state | rest] -> process_messages(messages, state, rest)
        _other -> response
      end
    end

    defp process_messages(messages, state, rest) do
      Polyn.Tracing.processing_span state.type do
        store_name = state.store_name
        messages = Enum.map(messages, &message_to_event(store_name, state, &1))

        handle_invalid_messages!(messages, state)

        [:noreply, messages, state | rest] |> List.to_tuple()
      end
    end

    defp message_to_event(store_name, %{type: type} = state, %Message{data: data} = message) do
      process_span_context = Polyn.Tracing.current_context()

      Polyn.Tracing.subscribe_span type, message.metadata[:headers] do
        Polyn.Tracing.link_to_context(process_span_context)

        case JSON.deserialize(data, store_name: store_name) do
          {:ok, event} ->
            Polyn.Tracing.span_attributes(
              conn: state.connection_options.connection_name,
              type: type,
              event: event,
              payload: data
            )

            Message.update_data(message, fn _data -> event end)

          {:error, error} ->
            Message.configure_ack(message, on_failure: :term)
            |> Message.failed(error)
        end
      end
    end

    defp handle_invalid_messages!(messages, %{ack_ref: ack_ref, acknowledger: acknowledger}) do
      if any_invalid?(messages) do
        # Treat all messages as failed since some are invalid. The ones that are valid
        # will send a NACK to indicate they weren't processed and should be sent again
        # the invalid ones will be given TERM so they aren't sent again
        acknowledger.ack(ack_ref, [], messages)

        raise Polyn.ValidationException, combine_invalid_message_errors(messages)
      end
    end

    defp any_invalid?(messages) do
      Enum.any?(messages, &message_invalid?/1)
    end

    defp message_invalid?(message), do: message.status != :ok

    defp combine_invalid_message_errors(messages) do
      Enum.filter(messages, &message_invalid?/1)
      |> Enum.map_join("\n", fn
        %{status: {:failed, reason}} -> reason
        _ -> ""
      end)
    end

    defp store_name(opts) do
      Keyword.get(opts, :store_name, SchemaStore.store_name())
    end

    defp add_consumer_and_stream_name(opts) do
      type = Keyword.fetch!(opts, :type)
      source = Keyword.get(opts, :source)
      conn = Keyword.get(opts, :connection_name)
      consumer_name = Polyn.Naming.consumer_name(type, source)
      stream_name = Polyn.Naming.lookup_stream_name!(conn, type)

      Keyword.put(opts, :stream_name, stream_name)
      |> Keyword.put(:consumer_name, consumer_name)
    end

    defp producer(opts) do
      if sandbox(opts), do: Polyn.Jetstream.MockProducer, else: OffBroadway.Jetstream.Producer
    end

    defp acknowledger(opts) do
      if sandbox(opts),
        do: Polyn.Jetstream.MockAcknowledger,
        else: OffBroadway.Jetstream.Acknowledger
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
end
