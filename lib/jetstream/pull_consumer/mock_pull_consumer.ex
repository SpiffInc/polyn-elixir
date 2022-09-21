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
        lookup_stream!(conn_opts)
        |> find_consumer_subjects(lookup_consumer!(conn_opts))
        |> subscribe_to_subjects()

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

  defp lookup_stream!(conn_opts) do
    Polyn.Jetstream.stream_info!(
      Keyword.fetch!(conn_opts, :connection_name),
      Keyword.fetch!(conn_opts, :stream_name)
    )
  end

  # This relies on a real look up on a running nats server.
  # This ensures that applications are using real consumer
  # names and real stream names that exist in their server
  defp lookup_consumer!(conn_opts) do
    Polyn.Jetstream.consumer_info!(
      Keyword.fetch!(conn_opts, :connection_name),
      Keyword.fetch!(conn_opts, :stream_name),
      Keyword.fetch!(conn_opts, :consumer_name)
    )
  end

  defp find_consumer_subjects(stream, consumer) do
    stream_subjects = stream.config.subjects
    consumer_subject = consumer.config.filter_subject

    case consumer_subject do
      nil -> stream_subjects
      subject -> [subject]
    end
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
  def handle_info({:msg, _msg} = msg, %{module: module, state: state} = internal_state) do
    module.handle_message(msg, state)
    {:noreply, internal_state}
  end
end
