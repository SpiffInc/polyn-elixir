with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule OffBroadway.Polyn.Producer do
    @moduledoc """
    A Broadway Producer for Polyn. It wraps `OffBroadway.Jetstream.Producer` and will validate
    that any messages coming through are valid events and follow the schema for the event
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

        {:error, _error} ->
          Message.configure_ack(message, on_failure: :term)
      end
    end

    defp store_name(opts) do
      Keyword.get(opts, :store_name, SchemaStore.store_name())
    end
  end
end
