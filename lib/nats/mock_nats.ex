defmodule Polyn.MockNats do
  @moduledoc false
  # Mock nats for isolated state in tests

  @behaviour Polyn.NatsBehaviour

  use GenServer

  defstruct messages: [], subscribers: [], consumers: []

  def start_link(arg \\ nil) do
    GenServer.start_link(__MODULE__, arg)
  end

  def get_messages(conn) do
    GenServer.call(conn, :get_messages)
  end

  @impl Polyn.NatsBehaviour
  def pub(conn, subject, data, opts \\ []) do
    GenServer.call(conn, {:pub, subject, data, opts})
    # Gnat.pub(conn, subject, data, opts)
  end

  @impl Polyn.NatsBehaviour
  def sub(conn, subject, opts \\ []) do
    # Gnat.sub(conn, subject, opts)
  end

  @impl Polyn.NatsBehaviour
  def request(conn, subject, data, opts \\ []) do
    # Gnat.request(conn, subject, data, opts)
  end

  @impl GenServer
  def init(_arg) do
    {:ok, %__MODULE__{}}
  end

  @impl GenServer
  def handle_call(:get_messages, _from, state) do
    {:reply, state.messages, state}
  end

  def handle_call({:pub, subject, data, opts}, _from, state) do
    messages =
      state.messages ++
        [
          %{
            gnat: self(),
            topic: subject,
            body: data,
            reply_to: Keyword.get(opts, :reply_to),
            headers: Keyword.get(opts, :headers)
          }
        ]

    {:reply, :ok, Map.put(state, :messages, messages)}
  end
end
