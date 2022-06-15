defmodule OffBroadway.Polyn.TransformerTest do
  use Polyn.ConnCase, async: true

  alias Jetstream.API.{Consumer, Stream}
  alias Polyn.{Event, SchemaStore}

  @conn_name :broadway_transformer_test
  @moduletag with_gnat: @conn_name
  @store_name "BROADWAY_TRANSFORMER_TEST_SCHEMA_STORE"
  @stream_name "BROADWAY_TRANSFORMER_TEST_STREAM"
  @stream_subjects ["company.created.v1"]
  @consumer_name "com_test_company_backend_company_created_v1"

  setup do
    SchemaStore.create_store(@conn_name, name: @store_name)

    add_schema("company.created.v1", %{
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

  defmodule ExampleBroadwayPipeline do
    use Broadway

    def start_link(opts) do
      Broadway.start_link(
        __MODULE__,
        name: __MODULE__,
        producer: [
          module: {
            OffBroadway.Jetstream.Producer,
            connection_name: :broadway_transformer_test,
            stream_name: "BROADWAY_TRANSFORMER_TEST_STREAM",
            consumer_name: "com_test_company_backend_company_created_v1"
          },
          transformer:
            {OffBroadway.Polyn.Transformer, :transform,
             connection_name: :broadway_transformer_test,
             store_name: "BROADWAY_TRANSFORMER_TEST_SCHEMA_STORE"}
        ],
        processors: [
          default: []
        ],
        context: %{test_pid: Keyword.get(opts, :test_pid)}
      )
    end

    def handle_message(_processor_name, message, context) do
      send(context.test_pid, {:received_event, message.data})
      message
    end
  end

  test "valid messages are converted to Event structs" do
    Gnat.pub(@conn_name, "company.created.v1", """
    {
      "type": "com.test.company.created.v1",
      "data": {
        "name": "Toph",
        "element": "earth"
      }
    }
    """)

    Gnat.pub(@conn_name, "company.created.v1", """
    {
      "type": "com.test.company.created.v1",
      "data": {
        "name": "Katara",
        "element": "water"
      }
    }
    """)

    start_pipeline()

    assert_receive(
      {:received_event,
       %Event{
         type: "com.test.company.created.v1",
         data: %{
           "name" => "Katara",
           "element" => "water"
         }
       }}
    )

    assert_receive(
      {:received_event,
       %Event{
         type: "com.test.company.created.v1",
         data: %{
           "name" => "Toph",
           "element" => "earth"
         }
       }}
    )
  end

  defp start_pipeline do
    start_supervised!({ExampleBroadwayPipeline, test_pid: self()})
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
