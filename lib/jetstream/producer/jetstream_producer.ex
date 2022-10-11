with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule Polyn.Jetstream.Producer do
    # Real calls to Offbroadway.Jetstream.Producer.
    # This exists to ensure we don't mock things we don't "own"
    @moduledoc false

    use GenStage

    @behaviour Broadway.Producer

    @impl GenStage
    defdelegate init(opts), to: OffBroadway.Jetstream.Producer

    @impl GenStage
    defdelegate handle_demand(incoming_demand, state), to: OffBroadway.Jetstream.Producer

    @impl Broadway.Producer
    defdelegate prepare_for_start(module, opts), to: OffBroadway.Jetstream.Producer

    @impl Broadway.Producer
    defdelegate prepare_for_draining(state), to: OffBroadway.Jetstream.Producer

    @impl GenStage
    defdelegate handle_info(any, state), to: OffBroadway.Jetstream.Producer
  end
end
