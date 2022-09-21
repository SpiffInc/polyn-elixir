defmodule Polyn.Jetstream.Producer do
  # Real calls to Offbroadway.Jetstream.Producer
  @moduledoc false

  use GenStage

  @impl GenStage
  def init(opts \\ []) do
    OffBroadway.Jetstream.Producer.init(opts)
  end

  @impl GenStage
  def handle_demand(incoming_demand, state) do
    OffBroadway.Jetstream.Producer.handle_demand(incoming_demand, state)
  end
end
