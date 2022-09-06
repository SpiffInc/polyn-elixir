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
  end

  @impl Polyn.NatsBehaviour
  def sub(conn, subscriber, subject, opts \\ []) do
    GenServer.call(conn, {:sub, subscriber, subject, opts})
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
    msg = %{
      gnat: self(),
      topic: subject,
      body: data,
      reply_to: Keyword.get(opts, :reply_to),
      headers: Keyword.get(opts, :headers)
    }

    messages = state.messages ++ [msg]

    Enum.filter(state.subscribers, &(&1.subject == subject))
    |> Enum.each(fn sub ->
      send(sub.subscriber, {:msg, Map.put(msg, :sid, sub.sid)})
    end)

    {:reply, :ok, Map.put(state, :messages, messages)}
  end

  def handle_call({:sub, subscriber, subject, _opts}, _from, state) do
    sid = System.unique_integer([:positive])

    subscribers =
      state.subscribers ++
        [%{sid: sid, subject: subject, subscriber: subscriber}]

    {:reply, {:ok, sid}, Map.put(state, :subscribers, subscribers)}
  end
end
