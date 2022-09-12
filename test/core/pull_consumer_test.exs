defmodule Polyn.PullConsumerTest do
  use Polyn.ConnCase, async: true

  alias Jetstream.API.{Consumer, Stream}
  alias Polyn.Event
  alias Polyn.SchemaStore

  @conn_name :pull_consumer_gnat
  @moduletag with_gnat: @conn_name
  @store_name "PULL_CONSUMER_TEST_SCHEMA_STORE"
  @stream_name "PULL_CONSUMER_TEST_STREAM"
  @stream_subjects ["pull.consumer.test.event.v1"]
  @consumer_name "user_backend_pull_consumer_test_event_v1"

  defmodule ExamplePullConsumer do
    use Polyn.PullConsumer

    def start_link(init_arg) do
      {store_name, init_arg} = Keyword.pop!(init_arg, :store_name)

      Polyn.PullConsumer.start_link(__MODULE__, init_arg,
        store_name: store_name,
        connection_name: :pull_consumer_gnat,
        type: "pull.consumer.test.event.v1"
      )
    end

    def init(init_arg) do
      {:ok, %{test_pid: Keyword.fetch!(init_arg, :test_pid)}}
    end

    def handle_message(event, _message, state) do
      send(state.test_pid, {:received_event, event})
      {:ack, state}
    end
  end

  setup do
    start_supervised!(
      {SchemaStore,
       [
         store_name: @store_name,
         connection_name: :foo,
         schemas: %{
           "pull.consumer.test.event.v1" =>
             Jason.encode!(%{
               "type" => "object",
               "required" => ["type"],
               "properties" => %{
                 "data" => %{
                   "type" => "object",
                   "properties" => %{
                     "name" => %{"type" => "string"},
                     "element" => %{"type" => "string"}
                   }
                 }
               }
             })
         }
       ]}
    )

    setup_stream()
    setup_consumer()

    on_exit(fn ->
      cleanup()
    end)
  end

  @tag capture_log: true
  test "receives a message" do
    Gnat.pub(@conn_name, "pull.consumer.test.event.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "type": "com.test.pull.consumer.test.event.v1",
      "specversion": "1.0.1",
      "data": {
        "name": "Toph",
        "element": "earth"
      }
    }
    """)

    Gnat.pub(@conn_name, "pull.consumer.test.event.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "specversion": "1.0.1",
      "type": "com.test.pull.consumer.test.event.v1",
      "data": {
        "name": "Katara",
        "element": "water"
      }
    }
    """)

    start_listening_for_messages()

    assert_receive(
      {:received_event,
       %Event{
         type: "com.test.pull.consumer.test.event.v1",
         data: %{
           "name" => "Katara",
           "element" => "water"
         }
       }}
    )

    assert_receive(
      {:received_event,
       %Event{
         type: "com.test.pull.consumer.test.event.v1",
         data: %{
           "name" => "Toph",
           "element" => "earth"
         }
       }}
    )
  end

  @tag capture_log: true
  test "errors if payload is invalid" do
    Gnat.pub(@conn_name, "pull.consumer.test.event.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "specversion": "1.0.1",
      "type": "com.test.pull.consumer.test.event.v1",
      "data": {
        "name": 123,
        "element": true
      }
    }
    """)

    pid = start_listening_for_messages()
    ref = Process.monitor(pid)

    assert_receive({:DOWN, ^ref, :process, ^pid, {%Polyn.ValidationException{}, _stack}})
  end

  @tag capture_log: true
  test "receives next message after error" do
    Gnat.pub(@conn_name, "pull.consumer.test.event.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "specversion": "1.0.1",
      "type": "com.test.pull.consumer.test.event.v1",
      "data": {
        "name": 123,
        "element": true
      }
    }
    """)

    Gnat.pub(@conn_name, "pull.consumer.test.event.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "type": "com.test.pull.consumer.test.event.v1",
      "specversion": "1.0.1",
      "data": {
        "name": "Toph",
        "element": "earth"
      }
    }
    """)

    pid = start_listening_for_messages()
    ref = Process.monitor(pid)

    assert_receive({:DOWN, ^ref, :process, ^pid, {%Polyn.ValidationException{}, _stack}})

    assert_receive(
      {:received_event,
       %Event{
         type: "com.test.pull.consumer.test.event.v1",
         data: %{
           "name" => "Toph",
           "element" => "earth"
         }
       }},
      2_000
    )
  end

  @tag capture_log: true
  test "errors if stream doesn't exist" do
    Stream.delete(@conn_name, @stream_name)

    Gnat.pub(@conn_name, "pull.consumer.test.event.v1", """
    {
      "id": "abc",
      "source": "com.test.foo",
      "type": "com.test.pull.consumer.test.event.v1",
      "specversion": "1.0.1",
      "data": {
        "name": "Toph",
        "element": "earth"
      }
    }
    """)

    # Catching runtime error because that's what happen when supervisor can't start a child_spec
    %{message: message} =
      assert_raise(RuntimeError, fn ->
        start_listening_for_messages()
      end)

    assert message =~ "Polyn.StreamException"
    assert message =~ "Could not find any streams for type pull.consumer.test.event.v1"

    # recreate these so the cleanup function works
    setup_stream()
    setup_consumer()
  end

  defp start_listening_for_messages do
    start_supervised!({ExamplePullConsumer, test_pid: self(), store_name: @store_name})
  end

  defp setup_stream do
    stream = %Stream{name: @stream_name, subjects: @stream_subjects}
    {:ok, _response} = Stream.create(@conn_name, stream)
  end

  defp setup_consumer do
    consumer = %Consumer{stream_name: @stream_name, durable_name: @consumer_name}
    {:ok, _response} = Consumer.create(@conn_name, consumer)
  end

  defp cleanup do
    # Manage connection on our own here, because all supervised processes will be
    # closed by the time `on_exit` runs
    {:ok, pid} = Gnat.start_link()
    :ok = Consumer.delete(pid, @stream_name, @consumer_name)
    :ok = Stream.delete(pid, @stream_name)
    Gnat.stop(pid)
  end
end
