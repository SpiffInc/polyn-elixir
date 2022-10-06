defmodule Polyn.Jetstream.MockProducer do
  # Mock calls to Offbroadway.Jetstream.Producer for testing isolation
  @moduledoc false

  use GenStage

  alias Broadway.Message
  alias Polyn.MockNats

  @impl GenStage
  def init(opts \\ []) do
    subjects =
      Polyn.Jetstream.subjects_for_consumer(
        Keyword.fetch!(opts, :connection_name),
        Keyword.fetch!(opts, :stream_name),
        Keyword.fetch!(opts, :consumer_name)
      )

    subscribe_to_subjects(subjects)

    messages = fetch_all_messages(subjects)

    {:producer, %{demand: 0, subjects: subjects, messages: messages}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    load_messages(%{state | demand: demand + incoming_demand})
  end

  defp load_messages(%{demand: demand} = state) when demand > 0 do
    {to_send, remaining} = Enum.split(state.messages, demand)

    {:noreply, to_send, %{state | messages: remaining}}
  end

  defp load_messages(state) do
    {:noreply, [], state}
  end

  defp fetch_all_messages(subjects) do
    MockNats.get_messages()
    |> Enum.filter(fn msg ->
      Enum.any?(subjects, &Polyn.Naming.subject_matches?(msg.topic, &1))
    end)
    |> Enum.map(&wrap_message/1)
  end

  defp wrap_message(msg) do
    %Message{
      data: msg.body,
      metadata: %{
        topic: msg.topic
      },
      acknowledger: {Broadway.NoopAcknowledger, nil, msg.body}
    }
  end

  defp subscribe_to_subjects(subjects) do
    Enum.each(subjects, fn subject ->
      Polyn.MockNats.sub(:foo, self(), subject)
    end)
  end

  # When a new message is published we'll add it to the list of messages
  @impl GenStage
  def handle_info({:msg, msg}, state) do
    load_messages(%{state | messages: state.messages ++ [wrap_message(msg)]})
  end
end
