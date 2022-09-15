defmodule Polyn.Jetstream.MockPullConsumerTest do
  # async: false because of singleton Sandbox
  use Polyn.ConnCase, async: false

  alias Jetstream.API.{Stream}
  alias Polyn.Jetstream.MockPullConsumer
  alias Polyn.Sandbox

  @conn_name :mock_pull_consumer_gnat
  @moduletag with_gnat: @conn_name
  @stream_name "MOCK_PULL_CONSUMER_TEST_STREAM"
  @stream_subject "mock.pull.consumer.test.event.v1"

  setup do
    start_supervised!(Sandbox)
    mock_nats = start_supervised!(Polyn.MockNats)
    Sandbox.setup_test(self(), mock_nats)

    stream = %Stream{name: @stream_name, subjects: [@stream_subject]}
    {:ok, _response} = Stream.create(@conn_name, stream)

    mock = start_mock()

    on_exit(fn ->
      cleanup(fn pid ->
        :ok = Stream.delete(pid, @stream_name)
      end)
    end)

    %{nats: mock_nats, mock: mock}
  end

  defmodule ExampleConsumer do
    def init(arg) do
      {:ok, arg}
    end
  end

  test "it initializes", %{mock: mock} do
    assert %{state: %Polyn.PullConsumer{}} = MockPullConsumer.get_state(mock)
  end

  defp start_mock do
    start_supervised!(%{
      :id => MockPullConsumer,
      :start => {
        MockPullConsumer,
        :start_link,
        # Same init args that Polyn.PullConsumer passes to Jetstream.PullConsumer
        [
          Polyn.PullConsumer,
          {Polyn.PullConsumer.new(ExampleConsumer,
             type: @stream_subject,
             connection_name: @conn_name
           ), nil}
        ]
      }
    })
  end
end
