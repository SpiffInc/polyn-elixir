defmodule Polyn.PullConsumerTest do
  # async false for global sandbox
  use Polyn.ConnCase, async: false
  use Polyn.TracingCase

  alias Jetstream.API.{Consumer, Stream}
  alias Polyn.Event
  alias Polyn.MockNats
  alias Polyn.Sandbox
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
      {config, init_arg} = Keyword.split(init_arg, [:store_name, :sandbox])

      Polyn.PullConsumer.start_link(__MODULE__, init_arg,
        store_name: Keyword.fetch!(config, :store_name),
        sandbox: Keyword.fetch!(config, :sandbox),
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
    start_supervised!(Sandbox)
    mock_nats = start_supervised!(MockNats)
    Sandbox.setup_test(self(), mock_nats)

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
      cleanup(fn pid ->
        :ok = Consumer.delete(pid, @stream_name, @consumer_name)
        :ok = Stream.delete(pid, @stream_name)
      end)
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

  test "adds tracing" do
    start_collecting_spans()

    Polyn.pub(
      @conn_name,
      "pull.consumer.test.event.v1",
      %{
        "name" => "Toph",
        "element" => "earth"
      },
      store_name: @store_name
    )

    start_listening_for_messages()

    {:received_event, event} =
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

    span_attrs =
      span_attributes(
        "pull.consumer.test.event.v1",
        event.id,
        Jason.encode!(Map.from_struct(event))
      )

    assert_receive(
      {:span,
       span_record(
         name: "pull.consumer.test.event.v1 receive",
         kind: "CONSUMER",
         attributes: ^span_attrs
       )}
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

    assert_receive({:DOWN, ^ref, :process, ^pid, {%Polyn.ValidationException{}, _stack}}, 500)
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

    assert_receive({:DOWN, ^ref, :process, ^pid, {%Polyn.ValidationException{}, _stack}}, 500)

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

  describe "mock integration" do
    test "receives a message" do
      MockNats.pub(@conn_name, "pull.consumer.test.event.v1", """
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

      start_listening_for_messages(sandbox: true)

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

    test "receives a message after start" do
      start_listening_for_messages(sandbox: true)

      MockNats.pub(@conn_name, "pull.consumer.test.event.v1", """
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
  end

  defp start_listening_for_messages(opts \\ []) do
    start_supervised!(
      {ExamplePullConsumer,
       test_pid: self(), store_name: @store_name, sandbox: Keyword.get(opts, :sandbox, false)}
    )
  end

  defp setup_stream do
    stream = %Stream{name: @stream_name, subjects: @stream_subjects}
    {:ok, _response} = Stream.create(@conn_name, stream)
  end

  defp setup_consumer do
    consumer = %Consumer{stream_name: @stream_name, durable_name: @consumer_name}
    {:ok, _response} = Consumer.create(@conn_name, consumer)
  end
end
