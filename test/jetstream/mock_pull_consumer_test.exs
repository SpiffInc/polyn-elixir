defmodule Polyn.Jetstream.MockPullConsumerTest do
  # async: false because of singleton Sandbox
  use Polyn.ConnCase, async: false

  alias Jetstream.API.{Consumer, Stream}
  alias Polyn.Jetstream.MockPullConsumer
  alias Polyn.MockNats
  alias Polyn.Sandbox

  @conn_name :mock_pull_consumer_gnat
  @moduletag with_gnat: @conn_name
  @stream_name "MOCK_PULL_CONSUMER_TEST_STREAM"
  @stream_subject "mock.pull.consumer.test.event.v1"
  @other_subject "mock.pull.consumer.test.>"
  @consumer_name "user_backend_mock_pull_consumer_test_event_v1"

  setup do
    start_supervised!(Sandbox)
    mock_nats = start_supervised!(MockNats)
    Sandbox.setup_test(self(), mock_nats)

    stream = %Stream{name: @stream_name, subjects: [@stream_subject, @other_subject]}
    {:ok, _response} = Stream.create(@conn_name, stream)
    consumer = %Consumer{stream_name: @stream_name, durable_name: @consumer_name}
    {:ok, _response} = Consumer.create(@conn_name, consumer)

    mock = start_mock()

    on_exit(fn ->
      cleanup(fn pid ->
        :ok = Consumer.delete(pid, @stream_name, @consumer_name)
        :ok = Stream.delete(pid, @stream_name)
      end)
    end)

    %{mock: mock}
  end

  defmodule ExampleConsumer do
    def init(test_pid) do
      {:ok, test_pid,
       connection_name: :mock_pull_consumer_gnat,
       stream_name: "MOCK_PULL_CONSUMER_TEST_STREAM",
       consumer_name: "user_backend_mock_pull_consumer_test_event_v1"}
    end

    def handle_message(message, test_pid) do
      send(test_pid, message)
      {:ack, test_pid}
    end
  end

  test "it initializes", %{mock: mock} do
    test_pid = self()
    assert %{module: ExampleConsumer, state: ^test_pid} = MockPullConsumer.get_state(mock)
  end

  test "it receives messages" do
    assert :ok = MockNats.pub(:foo, @stream_subject, "foo")
    assert :ok = MockNats.pub(:foo, "mock.pull.consumer.test.bar", "bar")

    assert_receive({:msg, %{topic: @stream_subject}})
    assert_receive({:msg, %{topic: "mock.pull.consumer.test.bar"}})
  end

  defp start_mock do
    start_supervised!(%{
      :id => MockPullConsumer,
      :start => {
        MockPullConsumer,
        :start_link,
        [ExampleConsumer, self()]
      }
    })
  end
end
