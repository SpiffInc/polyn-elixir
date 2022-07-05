with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule OffBroadway.Polyn.Producer do
    @moduledoc """
    A [Broadway](https://hexdocs.pm/broadway/Broadway.html) Producer for Polyn.

    The word `Producer` here is confusing because the word is overloaded.
    In this module `Producer` refers to [GenStage](https://hexdocs.pm/gen_stage/GenStage.html) data
    pipelines where a `:producer` is the stage that receives demand for data and sends it to a `:consumer`.
    This module isn't creating events to send to our NATS server. It doesn't "produce" new events for
    the rest of the system to consume. Rather it consumes events from a NATS Stream and passes them
    to GenStage `:consumer` modules you create.

    ## Usage

    This module wraps `OffBroadway.Jetstream.Producer` and will validate that any messages coming through
    are valid events and conform to the schema for the event. Use the `OffBroadway.Jetstream.Producer` documentation
    to learn how to use it. The only difference being you will use `OffBroadway.Polyn.Producer`
    in your `:module` configuration instead of the Jetsteram one. Invalid messages will send an ACKTERM
    to the NATS server so that they aren't sent again. They will be marked as `failed` and removed from the pipeline.
    """
    use GenStage

    alias Broadway.{Message, Producer}
    alias Polyn.SchemaStore

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

      {:noreply, messages, state}
    end

    defp message_to_event(conn, store_name, %Message{data: data} = message) do
      case Polyn.Serializers.JSON.deserialize(data, conn, store_name: store_name) do
        {:ok, event} ->
          Message.update_data(message, fn _data -> event end)

        {:error, error} ->
          Message.configure_ack(message, on_failure: :term)
          |> Message.failed(error)
      end
    end

    defp store_name(opts) do
      Keyword.get(opts, :store_name, SchemaStore.store_name())
    end
  end
end
