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
    to learn how to use it. The only difference being you will use `OffBroadway.Polyn.Producer`
    in your `:module` configuration instead of the Jetsteram one. Invalid messages will send an ACKTERM
    to the NATS server so that they aren't sent again. They will be marked as `failed` and removed from the pipeline.
    Valid messages that come in a batch with an invalid message will send a NACK response before an error
    is raised so that the NATS server will know they were received but need to be sent again
    """
    use GenStage

    alias Broadway.{Message, Producer}
    alias OffBroadway.Jetstream.Acknowledger
    alias Polyn.SchemaStore
    alias Polyn.Serializers.JSON

    @behaviour Producer

    @impl true
    defdelegate prepare_for_start(module, opts), to: OffBroadway.Jetstream.Producer

    @impl true
    defdelegate prepare_for_draining(state), to: OffBroadway.Jetstream.Producer

    @impl true
    defdelegate handle_info(any, state), to: OffBroadway.Jetstream.Producer

    @impl true
    def init(opts) do
      {:producer, state} = OffBroadway.Jetstream.Producer.init(opts)
      state = Map.put(state, :store_name, store_name(opts))
      {:producer, state}
    end

    @impl true
    def handle_demand(incoming_demand, state) do
      {:noreply, messages, state} =
        OffBroadway.Jetstream.Producer.handle_demand(incoming_demand, state)

      conn = state.connection_options.connection_name
      store_name = state.store_name

      messages = Enum.map(messages, &message_to_event(conn, store_name, &1))

      handle_invalid_messages!(messages, state.ack_ref)

      {:noreply, messages, state}
    end

    defp message_to_event(conn, store_name, %Message{data: data} = message) do
      case JSON.deserialize(data, conn, store_name: store_name) do
        {:ok, event} ->
          Message.update_data(message, fn _data -> event end)

        {:error, error} ->
          Message.configure_ack(message, on_failure: :term)
          |> Message.failed(error)
      end
    end

    defp handle_invalid_messages!(messages, ack_ref) do
      if any_invalid?(messages) do
        # Treat all messages as failed since some are invalid. The ones that are valid
        # will send a NACK to indicate they weren't processed and should be sent again
        # the invalid ones will be given TERM so they aren't sent again
        Acknowledger.ack(ack_ref, [], messages)

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
  end
end
