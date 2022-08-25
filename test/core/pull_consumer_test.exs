defmodule Polyn.PullConsumerTest do
  use Polyn.ConnCase, async: true

  alias Jetstream.API.{Consumer, Stream}
  alias Polyn.Event
  alias Polyn.SchemaStore

  @conn_name :pull_consumer_gnat
  @moduletag with_gnat: @conn_name
  @store_name "PULL_CONSUMER_TEST_SCHEMA_STORE"
  @stream_name "PULL_CONSUMER_TEST_STREAM"
  @stream_subjects ["user.created.v1"]
  @consumer_name "com_test_user_backend_user_created_v1"

  defmodule ExamplePullConsumer do
    use Polyn.PullConsumer

    def start_link(init_arg) do
      {store_name, init_arg} = Keyword.pop!(init_arg, :store_name)
      Polyn.PullConsumer.start_link(__MODULE__, init_arg, store_name: store_name)
    end

    def init(init_arg) do
      {test_pid, connection_options} = Keyword.pop!(init_arg, :test_pid)

      {:ok, %{test_pid: test_pid},
       Keyword.merge([connection_name: :pull_consumer_gnat], connection_options)}
    end

    def handle_message(event, _message, state) do
      send(state.test_pid, {:received_event, event})
      {:ack, state}
    end
  end

  setup do
    SchemaStore.create_store(@conn_name, name: @store_name)

    add_schema("user.created.v1", %{
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

    stream = %Stream{name: @stream_name, subjects: @stream_subjects}
    {:ok, _response} = Stream.create(@conn_name, stream)

    consumer = %Consumer{stream_name: @stream_name, durable_name: @consumer_name}
    {:ok, _response} = Consumer.create(@conn_name, consumer)

    on_exit(fn ->
      cleanup()
    end)
  end

  @tag capture_log: true
  test "receives a message" do
    Gnat.pub(@conn_name, "user.created.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "type": "com.test.user.created.v1",
      "specversion": "1.0.1",
      "type": "com.test.user.created.v1",
      "data": {
        "name": "Toph",
        "element": "earth"
      }
    }
    """)

    Gnat.pub(@conn_name, "user.created.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "type": "com.test.user.created.v1",
      "specversion": "1.0.1",
      "type": "com.test.user.created.v1",
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
         type: "com.test.user.created.v1",
         data: %{
           "name" => "Katara",
           "element" => "water"
         }
       }}
    )

    assert_receive(
      {:received_event,
       %Event{
         type: "com.test.user.created.v1",
         data: %{
           "name" => "Toph",
           "element" => "earth"
         }
       }}
    )
  end

  @tag capture_log: true
  test "errors if payload is invalid" do
    Gnat.pub(@conn_name, "user.created.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "type": "com.test.user.created.v1",
      "specversion": "1.0.1",
      "type": "com.test.user.created.v1",
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
    Gnat.pub(@conn_name, "user.created.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "type": "com.test.user.created.v1",
      "specversion": "1.0.1",
      "type": "com.test.user.created.v1",
      "data": {
        "name": 123,
        "element": true
      }
    }
    """)

    Gnat.pub(@conn_name, "user.created.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "type": "com.test.user.created.v1",
      "specversion": "1.0.1",
      "type": "com.test.user.created.v1",
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
         type: "com.test.user.created.v1",
         data: %{
           "name" => "Toph",
           "element" => "earth"
         }
       }},
      2_000
    )
  end

  defp start_listening_for_messages do
    start_supervised!(
      {ExamplePullConsumer,
       test_pid: self(),
       stream_name: @stream_name,
       consumer_name: @consumer_name,
       store_name: @store_name}
    )
  end

  defp add_schema(type, schema) do
    SchemaStore.save(@conn_name, type, schema, name: @store_name)
  end

  defp cleanup do
    # Manage connection on our own here, because all supervised processes will be
    # closed by the time `on_exit` runs
    {:ok, pid} = Gnat.start_link()
    :ok = SchemaStore.delete_store(pid, name: @store_name)
    :ok = Consumer.delete(pid, @stream_name, @consumer_name)
    :ok = Stream.delete(pid, @stream_name)
    Gnat.stop(pid)
  end
end
