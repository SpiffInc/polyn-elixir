defmodule Polyn.Jetstream.MockPullConsumer do
  # Mock Jetstream PullConsumer for testing in isolation
  @moduledoc false

  @behaviour Polyn.Jetstream.PullConsumerBehaviour

  use GenServer

  @impl Polyn.Jetstream.PullConsumerBehaviour
  def start_link(_module, init_arg, options \\ []) do
    GenServer.start_link(__MODULE__, {init_arg, lookup_nats()}, options)
  end

  @impl Polyn.Jetstream.PullConsumerBehaviour
  def start(_module, init_arg, options \\ []) do
    GenServer.start(__MODULE__, {init_arg, lookup_nats()}, options)
  end

  @impl Polyn.Jetstream.PullConsumerBehaviour
  def close(consumer) do
    GenServer.stop(consumer)
  end

  def get_state(pid) do
    GenServer.call(pid, :get_state)
  end

  def init({init_arg, nats}) do
    case Polyn.PullConsumer.init(init_arg) do
      {:ok, state, _conn_opts} ->
        Polyn.MockNats.sub(:foo, self(), elem(init_arg, 0).type)

        {:ok,
         %{
           state: state,
           init_arg: init_arg,
           nats: nats
         }}

      other ->
        other
    end
  end

  defp lookup_nats do
    Polyn.Sandbox.get!(self())
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end
end
