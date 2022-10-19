defmodule OffBroadway.Polyn.ProducerTest do
  # false for global sandbox
  use Polyn.ConnCase, async: false
  use Polyn.TracingCase

  alias Broadway.Message
  alias Jetstream.API.{Consumer, Stream}
  alias Polyn.{Event, SchemaStore}

  @conn_name :broadway_producer_test
  @moduletag with_gnat: @conn_name
  @store_name "BROADWAY_PRODUCER_TEST_SCHEMA_STORE"
  @stream_name "BROADWAY_PRODUCER_TEST_STREAM"
  @stream_subjects ["company.created.v1"]
  @consumer_name "user_backend_company_created_v1"

  setup do
    start_supervised!(Polyn.Sandbox)
    mock_nats = start_supervised!(Polyn.MockNats)
    Polyn.Sandbox.setup_test(self(), mock_nats)

    start_supervised!(
      {SchemaStore,
       [
         store_name: @store_name,
         connection_name: :foo,
         schemas: %{
           "company.created.v1" =>
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

    stream = %Stream{name: @stream_name, subjects: @stream_subjects}
    {:ok, _response} = Stream.create(@conn_name, stream)

    consumer = %Consumer{stream_name: @stream_name, durable_name: @consumer_name}
    {:ok, _response} = Consumer.create(@conn_name, consumer)

    on_exit(fn ->
      cleanup(fn pid ->
        :ok = Consumer.delete(pid, @stream_name, @consumer_name)
        :ok = Stream.delete(pid, @stream_name)
      end)
    end)
  end

  defmodule ExampleBroadwayPipeline do
    use Broadway

    def start_link(opts) do
      Broadway.start_link(
        __MODULE__,
        name: __MODULE__,
        producer: [
          module: {
            OffBroadway.Polyn.Producer,
            connection_name: :broadway_producer_test,
            type: "company.created.v1",
            store_name: "BROADWAY_PRODUCER_TEST_SCHEMA_STORE",
            receive_interval: 1,
            sandbox: Keyword.get(opts, :sandbox)
          }
        ],
        processors: [
          default: [concurrency: 1]
        ],
        context: %{test_pid: Keyword.get(opts, :test_pid)}
      )
    end

    def handle_message(_processor_name, message, context) do
      send(context.test_pid, {:received_event, message})
      message
    end
  end

  describe "nats integration" do
    test "valid messages are converted to Event structs" do
      Gnat.pub(
        @conn_name,
        "company.created.v1",
        """
        {
          "id": "#{UUID.uuid4()}",
          "source": "com.test.foo",
          "type": "com.test.company.created.v1",
          "specversion": "1.0.1",
          "type": "com.test.company.created.v1",
          "data": {
            "name": "Toph",
            "element": "earth"
          }
        }
        """,
        headers: []
      )

      Gnat.pub(
        @conn_name,
        "company.created.v1",
        """
        {
          "id": "#{UUID.uuid4()}",
          "source": "com.test.foo",
          "type": "com.test.company.created.v1",
          "specversion": "1.0.1",
          "type": "com.test.company.created.v1",
          "data": {
            "name": "Katara",
            "element": "water"
          }
        }
        """,
        headers: []
      )

      start_pipeline()

      assert_receive(
        {:received_event,
         %Message{
           data: %Event{
             type: "com.test.company.created.v1",
             data: %{
               "name" => "Katara",
               "element" => "water"
             }
           }
         }}
      )

      assert_receive(
        {:received_event,
         %Message{
           data: %Event{
             type: "com.test.company.created.v1",
             data: %{
               "name" => "Toph",
               "element" => "earth"
             }
           }
         }}
      )
    end

    test "valid messages are received after start" do
      start_pipeline()

      Gnat.pub(@conn_name, "company.created.v1", """
      {
        "id": "#{UUID.uuid4()}",
        "source": "com.test.foo",
        "type": "com.test.company.created.v1",
        "specversion": "1.0.1",
        "data": {
          "name": "Toph",
          "element": "earth"
        }
      }
      """)

      assert_receive(
        {:received_event,
         %Message{
           data: %Event{
             type: "com.test.company.created.v1",
             data: %{
               "name" => "Toph",
               "element" => "earth"
             }
           }
         }}
      )
    end

    @tag capture_log: true
    test "invalid message is ACKTERM and raises" do
      bad_msg_id = UUID.uuid4()

      Gnat.pub(@conn_name, "company.created.v1", """
      {
        "id": "#{bad_msg_id}",
        "source": "com.test.foo",
        "type": "com.test.company.created.v1",
        "specversion": "1.0.1",
        "data": {
          "name": 123,
          "element": true
        }
      }
      """)

      Gnat.pub(@conn_name, "company.created.v1", """
      {
        "id": "#{UUID.uuid4()}",
        "source": "com.test.foo",
        "type": "com.test.company.created.v1",
        "specversion": "1.0.1",
        "data": {
          "name": "Toph",
          "element": "earth"
        }
      }
      """)

      Gnat.sub(@conn_name, self(), "$JS.ACK.#{@stream_name}.#{@consumer_name}.>")

      start_pipeline()

      pid =
        Broadway.producer_names(ExampleBroadwayPipeline)
        |> Enum.at(0)
        |> Process.whereis()

      ref = Process.monitor(pid)

      {:DOWN, _ref, :process, _pid, {%{message: message}, _stack}} =
        assert_receive({:DOWN, ^ref, :process, ^pid, {%Polyn.ValidationException{}, _stack}})

      assert message =~ "Polyn event #{bad_msg_id} from com.test.foo is not valid"

      refute_receive(
        {:received_event,
         %Message{
           data: %Event{
             type: "com.test.company.created.v1",
             data: %{
               "name" => "Toph",
               "element" => "earth"
             }
           }
         }}
      )

      assert_receive({:msg, %{body: "+TERM"}})
      assert_receive({:msg, %{body: "-NAK"}})
    end

    test "tracing" do
      start_collecting_spans()

      Polyn.pub(
        @conn_name,
        "company.created.v1",
        %{
          "name" => "Katara",
          "element" => "water"
        },
        store_name: @store_name
      )

      start_pipeline()

      {:received_event, msg} =
        assert_receive(
          {:received_event,
           %Message{
             data: %Event{
               type: "com.test.company.created.v1",
               data: %{
                 "name" => "Katara",
                 "element" => "water"
               }
             }
           }}
        )

      {:span, span_record(span_id: send_span_id)} =
        assert_receive(
          {:span,
           span_record(
             name: "company.created.v1 send",
             kind: "PRODUCER"
           )}
        )

      assert_receive(
        {:span,
         span_record(
           name: "company.created.v1 process",
           kind: "CONSUMER"
         )}
      )

      span_attrs =
        span_attributes(
          "company.created.v1",
          msg.data.id,
          Jason.encode!(Map.from_struct(msg.data))
        )

      assert_receive(
        {:span,
         span_record(
           name: "company.created.v1 receive",
           kind: "CONSUMER",
           attributes: ^span_attrs,
           parent_span_id: ^send_span_id
         )}
      )
    end
  end

  describe "mock integration" do
    test "valid messages are converted to Event structs" do
      Polyn.MockNats.pub(@conn_name, "company.created.v1", """
      {
        "id": "#{UUID.uuid4()}",
        "source": "com.test.foo",
        "type": "com.test.company.created.v1",
        "specversion": "1.0.1",
        "data": {
          "name": "Toph",
          "element": "earth"
        }
      }
      """)

      start_pipeline(sandbox: true)

      assert_receive(
        {:received_event,
         %Message{
           data: %Event{
             type: "com.test.company.created.v1",
             data: %{
               "name" => "Toph",
               "element" => "earth"
             }
           }
         }}
      )
    end

    test "valid messages are received after start" do
      start_pipeline(sandbox: true)

      Polyn.MockNats.pub(@conn_name, "company.created.v1", """
      {
        "id": "#{UUID.uuid4()}",
        "source": "com.test.foo",
        "type": "com.test.company.created.v1",
        "specversion": "1.0.1",
        "data": {
          "name": "Toph",
          "element": "earth"
        }
      }
      """)

      assert_receive(
        {:received_event,
         %Message{
           data: %Event{
             type: "com.test.company.created.v1",
             data: %{
               "name" => "Toph",
               "element" => "earth"
             }
           }
         }}
      )
    end

    @tag capture_log: true
    test "invalid message is ACKTERM and raises" do
      bad_msg_id = UUID.uuid4()

      Polyn.MockNats.pub(@conn_name, "company.created.v1", """
      {
        "id": "#{bad_msg_id}",
        "source": "com.test.foo",
        "type": "com.test.company.created.v1",
        "specversion": "1.0.1",
        "data": {
          "name": 123,
          "element": true
        }
      }
      """)

      start_pipeline(sandbox: true)

      pid =
        Broadway.producer_names(ExampleBroadwayPipeline)
        |> Enum.at(0)
        |> Process.whereis()

      ref = Process.monitor(pid)

      {:DOWN, _ref, :process, _pid, {%{message: message}, _stack}} =
        assert_receive({:DOWN, ^ref, :process, ^pid, {%Polyn.ValidationException{}, _stack}})

      assert message =~ "Polyn event #{bad_msg_id} from com.test.foo is not valid"
    end
  end

  defp start_pipeline(opts \\ []) do
    start_supervised!(
      {ExampleBroadwayPipeline, test_pid: self(), sandbox: Keyword.get(opts, :sandbox, false)}
    )
  end
end
