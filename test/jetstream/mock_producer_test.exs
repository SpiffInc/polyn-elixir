defmodule Polyn.Jetstream.MockProducerTest do
  # Async false because of global sandbox
  use Polyn.ConnCase, async: false

  alias Jetstream.API.{Consumer, Stream}
  alias Polyn.Jetstream.MockProducer
  alias Polyn.MockNats
  alias Polyn.Sandbox

  @conn_name :mock_producer_gnat
  @moduletag with_gnat: @conn_name
  @stream_name "MOCK_PRODUCER_TEST_STREAM"
  @stream_subject "mock.producer.test.event.v1"
  @other_subject "mock.producer.test.>"
  @consumer_name "user_backend_mock_producer_test_event_v1"

  setup do
    start_supervised!(Sandbox)
    mock_nats = start_supervised!(MockNats)
    Sandbox.setup_test(self(), mock_nats)

    stream = %Stream{name: @stream_name, subjects: [@stream_subject, @other_subject]}
    {:ok, _response} = Stream.create(@conn_name, stream)
    consumer = %Consumer{stream_name: @stream_name, durable_name: @consumer_name}
    {:ok, _response} = Consumer.create(@conn_name, consumer)

    on_exit(fn ->
      cleanup(fn pid ->
        :ok = Consumer.delete(pid, @stream_name, @consumer_name)
        :ok = Stream.delete(pid, @stream_name)
      end)
    end)

    :ok
  end

  defmodule ExampleBroadwayPipeline do
    use Broadway

    def start_link(opts) do
      Broadway.start_link(
        __MODULE__,
        name: __MODULE__,
        producer: [
          module: {
            MockProducer,
            connection_name: :mock_producer_gnat,
            stream_name: "MOCK_PRODUCER_TEST_STREAM",
            consumer_name: "user_backend_mock_producer_test_event_v1"
          }
        ],
        processors: [
          default: [concurrency: 1, min_demand: 9, max_demand: 10]
        ],
        context: %{test_pid: Keyword.get(opts, :test_pid)}
      )
    end

    def handle_message(_processor_name, message, context) do
      send(context.test_pid, message)
      message
    end
  end

  test "it receives already published messages" do
    assert :ok = MockNats.pub(:foo, @stream_subject, "foo")
    assert :ok = MockNats.pub(:foo, "mock.producer.test.bar", "bar")

    start_pipeline()

    assert_receive(%Broadway.Message{
      data: "foo",
      metadata: %{topic: "mock.producer.test.event.v1"},
      status: :ok
    })

    assert_receive(%Broadway.Message{
      data: "bar",
      metadata: %{topic: "mock.producer.test.bar"},
      status: :ok
    })
  end

  test "it published messages after the fact" do
    start_pipeline()

    assert :ok = MockNats.pub(:foo, @stream_subject, "foo")
    assert :ok = MockNats.pub(:foo, "mock.producer.test.bar", "bar")

    assert_receive(%Broadway.Message{
      data: "foo",
      metadata: %{topic: "mock.producer.test.event.v1"},
      status: :ok
    })

    assert_receive(%Broadway.Message{
      data: "bar",
      metadata: %{topic: "mock.producer.test.bar"},
      status: :ok
    })
  end

  defp start_pipeline do
    start_supervised!({ExampleBroadwayPipeline, test_pid: self()})
  end
end
