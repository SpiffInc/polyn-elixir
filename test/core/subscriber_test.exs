defmodule Polyn.SubscriberTest do
  # async false for global sandbox
  use Polyn.ConnCase, async: false
  use Polyn.TracingCase

  alias Polyn.{Event, MockNats, Sandbox, SchemaStore, Subscriber}

  @conn_name :subscriber_gnat
  @moduletag with_gnat: @conn_name

  @store_name "SUBSCRIBER_TEST_STORE"

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
        store_name: "SUBSCRIBER_TEST_STORE",
        sandbox: args[:sandbox]
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
    start_subscriber()

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
    pid = start_subscriber()
    ref = Process.monitor(pid)

    Gnat.pub(
      @conn_name,
      "subscriber.test.event.v1",
      """
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
      """
    )

    assert_receive({:DOWN, ^ref, :process, ^pid, {%Polyn.ValidationException{}, _stack}}, 500)
  end

  test "adds tracing" do
    start_collecting_spans()
    start_subscriber()

    Polyn.pub(@conn_name, "subscriber.test.event.v1", %{name: "Iroh", element: "fire"},
      reply_to: "foo",
      store_name: @store_name
    )

    {:received_event, event, msg} =
      assert_receive(
        {:received_event, %Event{data: %{"name" => "Iroh", "element" => "fire"}},
         %{reply_to: "foo"}}
      )

    span_attrs = span_attributes("subscriber.test.event.v1", event.id, msg.body)

    assert_receive(
      {:span,
       span_record(
         name: "subscriber.test.event.v1 receive",
         kind: "CONSUMER",
         attributes: ^span_attrs
       )}
    )
  end

  describe "mock integration" do
    test "receives messages" do
      start_subscriber(sandbox: true)

      MockNats.pub(
        @conn_name,
        "subscriber.test.event.v1",
        Jason.encode!(%{
          id: "foo",
          source: "com:test:user:backend",
          specversion: "1.0.1",
          type: "com.test.subscriber.test.event.v1",
          data: %{name: "Iroh", element: "fire"}
        }),
        reply_to: "foo"
      )

      assert_receive(
        {:received_event, %Event{data: %{"name" => "Iroh", "element" => "fire"}},
         %{reply_to: "foo"}}
      )
    end
  end

  defp start_subscriber(opts \\ []) do
    start_supervised!(
      {ExampleSubscriber, %{test_pid: self(), sandbox: Keyword.get(opts, :sandbox, false)}}
    )
  end
end
