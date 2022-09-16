defmodule Polyn.MockNats do
  @moduledoc false
  # Mock nats for isolated state in tests

  @behaviour Polyn.NatsBehaviour

  use GenServer

  defstruct messages: [], subscribers: %{}

  def start_link(arg \\ nil) do
    GenServer.start_link(__MODULE__, arg)
  end

  def get_messages do
    GenServer.call(lookup_nats_server(), :get_messages)
  end

  def get_subscribers do
    GenServer.call(lookup_nats_server(), :get_subscribers)
  end

  def get_state do
    GenServer.call(lookup_nats_server(), :get_state)
  end

  @impl Polyn.NatsBehaviour
  def pub(_conn, subject, data, opts \\ []) do
    GenServer.call(lookup_nats_server(), {:pub, subject, data, opts})
  end

  @impl Polyn.NatsBehaviour
  def sub(_conn, subscriber, subject, opts \\ []) do
    GenServer.call(lookup_nats_server(), {:sub, subscriber, subject, opts})
  end

  @impl Polyn.NatsBehaviour
  def unsub(_conn, sid, opts \\ []) do
    GenServer.call(lookup_nats_server(), {:unsub, sid, opts})
  end

  @impl Polyn.NatsBehaviour
  def request(conn, subject, data, opts \\ []) do
    {:ok, inbox} = GenServer.call(lookup_nats_server(), {:request, self(), subject, data, opts})

    result =
      receive do
        {:msg, %{topic: ^inbox} = msg} ->
          {:ok, msg}
      after
        100 ->
          {:error, :timeout}
      end

    :ok = unsub(conn, inbox)

    result
  end

  defp lookup_nats_server do
    Polyn.Sandbox.get!(self())
  end

  @impl GenServer
  def init(_arg) do
    {:ok, %__MODULE__{}}
  end

  @impl GenServer
  def handle_call(:get_messages, _from, state) do
    {:reply, state.messages, state}
  end

  def handle_call(:get_subscribers, _from, state) do
    {:reply, state.subscribers, state}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:pub, subject, data, opts}, _from, state) do
    state = publish_msg(subject, data, opts, state)

    {:reply, :ok, state}
  end

  def handle_call({:sub, subscriber, subject, _opts}, _from, state) do
    {sid, state} = add_subscriber(subscriber, subject, unique_integer(), state)

    {:reply, {:ok, sid}, state}
  end

  def handle_call({:unsub, sid, _opts}, _from, state) do
    subscribers = Map.delete(state.subscribers, sid)

    {:reply, :ok, %{state | subscribers: subscribers}}
  end

  def handle_call({:request, recipient, subject, data, _opts}, _from, state) do
    inbox = "_INBOX.#{unique_integer()}"
    # Use the INBOX as the sid on request subscriptions so it's easier to
    # find and delete the subscription after response
    {_sid, state} = add_subscriber(recipient, inbox, inbox, state)
    state = publish_msg(subject, data, [reply_to: inbox], state)

    {:reply, {:ok, inbox}, state}
  end

  defp publish_msg(subject, data, opts, state) do
    msg = %{
      gnat: self(),
      topic: subject,
      body: data,
      reply_to: Keyword.get(opts, :reply_to),
      headers: Keyword.get(opts, :headers)
    }

    messages = state.messages ++ [msg]

    send_to_subscribers(msg, state)

    Map.put(state, :messages, messages)
  end

  defp send_to_subscribers(msg, state) do
    Enum.filter(state.subscribers, fn {_key, sub} ->
      Polyn.Naming.subject_matches?(msg.topic, sub.subject)
    end)
    |> Enum.each(fn {sid, sub} ->
      send(sub.subscriber, {:msg, Map.put(msg, :sid, sid)})
    end)
  end

  defp add_subscriber(subscriber, subject, sid, state) do
    subscribers =
      Map.put(state.subscribers, sid, %{sid: sid, subject: subject, subscriber: subscriber})

    {sid, Map.put(state, :subscribers, subscribers)}
  end

  defp unique_integer do
    System.unique_integer([:positive])
  end
end
