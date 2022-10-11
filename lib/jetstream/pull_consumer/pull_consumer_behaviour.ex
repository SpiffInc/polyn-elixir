defmodule Polyn.Jetstream.PullConsumerBehaviour do
  @moduledoc false

  @callback start_link(module(), init_arg :: term(), options :: GenServer.options()) ::
              GenServer.on_start()
  @callback start(module(), init_arg :: term(), options :: GenServer.options()) ::
              GenServer.on_start()
  @callback close(consumer :: Jetstream.PullConsumer.consumer()) :: :ok
end
