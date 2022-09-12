defmodule Polyn.SubscriberTest do
  use Polyn.ConnCase, async: true

  alias Polyn.{Event, SchemaStore, Subscriber}

  @conn_name :subscriber_gnat
  @moduletag with_gnat: @conn_name

  @store_name "SUBSCRIBER_TEST_STORE"

  setup do
    start_supervised!(
      {SchemaStore,
       [
         store_name: @store_name,
         connection_name: :foo,
         schemas: %{
           "subscriber.test.event.v1" =>
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

    :ok
  end

  defmodule ExampleSubscriber do
    use Polyn.Subscriber

    def start_link(args) do
      Subscriber.start_link(__MODULE__, args,
        connection_name: :subscriber_gnat,
        event: "subscriber.test.event.v1",
        store_name: "SUBSCRIBER_TEST_STORE"
      )
    end

    def init(args) do
      {:ok, args}
    end

    def handle_message(event, message, state) do
      send(state.test_pid, {:received_event, event, message})
      {:noreply, state}
    end
  end

  test "receives messages" do
    start_supervised!({ExampleSubscriber, %{test_pid: self()}})

    Polyn.pub(@conn_name, "subscriber.test.event.v1", %{name: "Iroh", element: "fire"},
      reply_to: "foo",
      store_name: @store_name
    )

    assert_receive(
      {:received_event, %Event{data: %{"name" => "Iroh", "element" => "fire"}},
       %{reply_to: "foo"}}
    )

    Polyn.pub(@conn_name, "subscriber.test.event.v1", %{name: "Toph", element: "earth"},
      reply_to: "foo",
      store_name: @store_name
    )

    assert_receive(
      {:received_event, %Event{data: %{"name" => "Toph", "element" => "earth"}},
       %{reply_to: "foo"}}
    )
  end

  @tag capture_log: true
  test "raises if message is invalid" do
    pid = start_supervised!({ExampleSubscriber, %{test_pid: self()}})
    ref = Process.monitor(pid)

    Gnat.pub(@conn_name, "subscriber.test.event.v1", """
    {
      "id": "#{UUID.uuid4()}",
      "source": "com.test.foo",
      "type": "com.test.subscriber.test.event.v1",
      "specversion": "1.0.1",
      "data": {
        "name": 123,
        "element": 456
      }
    }
    """)

    assert_receive({:DOWN, ^ref, :process, ^pid, {%Polyn.ValidationException{}, _stack}})
  end
end
