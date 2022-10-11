defmodule Polyn.Jetstream.PullConsumer do
  # Real Jetstream PullConsumer for production
  # This exists to ensure we don't mock things we don't "own"
  @moduledoc false

  @behaviour Polyn.Jetstream.PullConsumerBehaviour

  defdelegate start_link(module, init_arg, options \\ []), to: Jetstream.PullConsumer
  defdelegate start(module, init_arg, options \\ []), to: Jetstream.PullConsumer
  defdelegate close(consumer), to: Jetstream.PullConsumer
end
