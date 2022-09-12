defmodule Polyn.Jetstream.PullConsumer do
  # Mock Jetstream PullConsumer for testing in isolation
  @moduledoc false

  @behaviour Polyn.Jetstream.PullConsumerBehaviour

  use GenServer

  @impl Polyn.Jetstream.PullConsumerBehaviour
  def start_link(module, init_arg, options \\ []) do
    GenServer.start_link(__MODULE__, {module, init_arg}, options)
  end

  @impl Polyn.Jetstream.PullConsumerBehaviour
  def start(module, init_arg, options \\ []) do
    GenServer.start(__MODULE__, {module, init_arg}, options)
  end

  @impl Polyn.Jetstream.PullConsumerBehaviour
  def close(consumer) do
    GenServer.stop(consumer)
  end
end
