defmodule Polyn.Jetstream.MockPullConsumer do
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

  def get_state(pid) do
    GenServer.call(pid, :get_state)
  end

  @impl GenServer
  def init({module, init_arg}) do
    case module.init(init_arg) do
      {:ok, state, conn_opts} ->
        # This relies on a real look up on a running nats server.
        # This ensures that applications are using real consumer
        # names and real stream names that exist in their server
        subjects =
          Polyn.Jetstream.subjects_for_consumer(
            Keyword.fetch!(conn_opts, :connection_name),
            Keyword.fetch!(conn_opts, :stream_name),
            Keyword.fetch!(conn_opts, :consumer_name)
          )

        subscribe_to_subjects(subjects)

        fetch_all_messages(subjects)
        |> Enum.each(fn msg ->
          send(self(), {:msg, msg})
        end)

        {:ok,
         %{
           module: module,
           state: state,
           init_arg: init_arg
         }}

      other ->
        other
    end
  end

  defp fetch_all_messages(subjects) do
    Polyn.MockNats.get_messages()
    |> Enum.filter(fn msg ->
      Enum.any?(subjects, &Polyn.Naming.subject_matches?(msg.topic, &1))
    end)
  end

  defp subscribe_to_subjects(subjects) do
    Enum.each(subjects, fn subject ->
      Polyn.MockNats.sub(:foo, self(), subject)
    end)
  end

  @impl GenServer
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl GenServer
  def handle_info({:msg, msg}, %{module: module, state: state} = internal_state) do
    module.handle_message(msg, state)
    {:noreply, internal_state}
  end
end
