defmodule PolynTest do
  use Polyn.ConnCase, async: true
  use Polyn.TracingCase

  alias Polyn.SchemaStore

  @conn_name :polyn_gnat
  @moduletag with_gnat: @conn_name

  @store_name "POLYN_TEST_SCHEMA_STORE"

  setup do
    start_supervised!(
      {SchemaStore,
       [
         store_name: @store_name,
         connection_name: :foo,
         schemas: %{
           "pub.test.event.v1" =>
             Jason.encode!(%{
               "type" => "object",
               "properties" => %{"data" => %{"type" => "string"}}
             }),
           "reply.test.event.v1" =>
             Jason.encode!(%{
               "type" => "object",
               "properties" => %{"data" => %{"type" => "string"}}
             }),
           "request.test.request.v1" =>
             Jason.encode!(%{
               "type" => "object",
               "properties" => %{"data" => %{"type" => "string"}}
             }),
           "request.test.response.v1" =>
             Jason.encode!(%{
               "type" => "object",
               "properties" => %{"data" => %{"type" => "string"}}
             })
         }
       ]}
    )

    :ok
  end

  describe "pub/4" do
    test "adds a new event to the server" do
      Gnat.sub(@conn_name, self(), "pub.test.event.v1")
      Polyn.pub(@conn_name, "pub.test.event.v1", "foo", store_name: @store_name)

      data = get_message() |> decode_message()
      assert data["data"] == "foo"
      assert data["datacontenttype"] == "application/json"
      assert data["source"] == "com:test:user:backend"
      assert data["specversion"] == "1.0.1"
      assert data["type"] == "com.test.pub.test.event.v1"
    end

    test "can include extra `source` info" do
      Gnat.sub(@conn_name, self(), "pub.test.event.v1")
      Polyn.pub(@conn_name, "pub.test.event.v1", "foo", store_name: @store_name, source: "orders")

      data = get_message() |> decode_message()
      assert data["source"] == "com:test:user:backend:orders"
    end

    test "makes a tracing span" do
      start_collecting_spans()

      Gnat.sub(@conn_name, self(), "pub.test.event.v1")
      Polyn.pub(@conn_name, "pub.test.event.v1", "foo", store_name: @store_name, source: "orders")

      msg = get_message()
      data = decode_message(msg)

      span_attrs = span_attributes("pub.test.event.v1", data["id"], msg.body)

      assert_receive(
        {:span,
         span_record(name: "pub.test.event.v1 send", kind: "PRODUCER", attributes: ^span_attrs)}
      )

      assert has_traceparent_header?(msg.headers)
    end

    test "raises if doesn't match schema" do
      start_collecting_spans()

      assert_raise(Polyn.ValidationException, fn ->
        Polyn.pub(@conn_name, "pub.test.event.v1", 100, store_name: @store_name, source: "orders")
      end)

      {:span, span} =
        assert_receive({:span, span_record(name: "pub.test.event.v1 send", kind: "PRODUCER")})

      event = get_events(span) |> Enum.at(0)

      assert event[:name] == "exception"
      assert event[:attributes]["exception.message"] =~ "Expected String but got Integer"
      assert event[:attributes]["exception.type"] =~ "Polyn.ValidationException"
      assert event[:attributes]["exception.stacktrace"] =~ "Polyn.Serializers.JSON.validate!/2"
    end

    test "passes through other options" do
      Gnat.sub(@conn_name, self(), "pub.test.event.v1")
      Polyn.pub(@conn_name, "pub.test.event.v1", "foo", store_name: @store_name, reply_to: "foo")

      %{reply_to: reply_to} = get_message()
      assert reply_to == "foo"
    end

    test "includes Nats-Msg-Id header" do
      Gnat.sub(@conn_name, self(), "pub.test.event.v1")

      Polyn.pub(@conn_name, "pub.test.event.v1", "foo",
        store_name: @store_name,
        headers: [{"foo", "bar"}]
      )

      msg = get_message()
      data = decode_message(msg)

      assert {"foo", "bar"} in msg.headers
      assert {"nats-msg-id", data["id"]} in msg.headers
    end
  end

  describe "reply/5" do
    test "replies to a message" do
      Gnat.sub(@conn_name, self(), "INBOX.me")
      Polyn.reply(@conn_name, "INBOX.me", "reply.test.event.v1", "foo", store_name: @store_name)

      data = get_message() |> decode_message()
      assert data["data"] == "foo"
    end

    test "generates traces" do
      start_collecting_spans()

      Gnat.sub(@conn_name, self(), "INBOX.me")
      Polyn.reply(@conn_name, "INBOX.me", "reply.test.event.v1", "foo", store_name: @store_name)

      msg = get_message()
      data = decode_message(msg)

      assert has_traceparent_header?(msg.headers)

      span_attrs = span_attributes("(temporary)", data["id"], msg.body)

      assert_receive(
        {:span,
         span_record(
           name: "(temporary) send",
           kind: "PRODUCER",
           attributes: ^span_attrs
         )}
      )
    end

    test "raises if doesn't match schema" do
      assert_raise(Polyn.ValidationException, fn ->
        Polyn.reply(@conn_name, "INBOX.me", "reply.test.event.v1", 100, store_name: @store_name)
      end)
    end
  end

  describe "request/4" do
    test "returned message is an event" do
      pid = spawn_reply_process("bar")

      {:ok, %{body: event}} =
        Polyn.request(@conn_name, "request.test.request.v1", "foo", store_name: @store_name)

      assert event.data == "bar"

      Process.exit(pid, :kill)
    end

    test "generates traces" do
      start_collecting_spans()
      Gnat.sub(@conn_name, self(), "request.test.request.v1")

      pid = spawn_reply_process("bar")

      {:ok, %{body: resp_event}} =
        Polyn.request(@conn_name, "request.test.request.v1", "foo", store_name: @store_name)

      req_msg = get_message()
      data = decode_message(req_msg)

      assert has_traceparent_header?(req_msg.headers)

      req_attrs = span_attributes("request.test.request.v1", data["id"], req_msg.body)

      assert_receive(
        {:span,
         span_record(
           name: "request.test.request.v1 send",
           kind: "PRODUCER",
           attributes: ^req_attrs
         )}
      )

      {:span, span_record(span_id: reply_span_id)} =
        assert_receive(
          {:span,
           span_record(
             name: "(temporary) send",
             kind: "PRODUCER"
           )}
        )

      resp_attrs =
        span_attributes("(temporary)", resp_event.id, Jason.encode!(Map.from_struct(resp_event)))

      assert_receive(
        {:span,
         span_record(
           name: "(temporary) receive",
           kind: "CONSUMER",
           parent_span_id: ^reply_span_id,
           attributes: ^resp_attrs
         )}
      )

      Process.exit(pid, :kill)
    end

    test "timeout is in span exceptions" do
      start_collecting_spans()
      Gnat.sub(@conn_name, self(), "request.test.request.v1")

      pid =
        spawn_reply_process("bar", fn reply_to, _return_value ->
          # Force reply to take longer that receive_timeout
          :timer.sleep(10)
          default_reply("bar", reply_to)
        end)

      assert {:error, :timeout} =
               Polyn.request(@conn_name, "request.test.request.v1", "foo",
                 store_name: @store_name,
                 receive_timeout: 1
               )

      {:span, span} =
        assert_receive(
          {:span,
           span_record(
             name: "request.test.request.v1 send",
             kind: "PRODUCER"
           )}
        )

      event = get_events(span) |> Enum.at(0)

      assert event[:name] == "exception"

      assert event[:attributes]["exception.message"] =~
               "request for request.test.request.v1 timeout"

      assert event[:attributes]["exception.type"] =~ "RuntimeError"
      assert event[:attributes]["exception.stacktrace"] =~ "Polyn.request/4"

      Process.exit(pid, :kill)
    end

    test "error if request event doesn't match schema" do
      assert_raise(Polyn.ValidationException, fn ->
        Polyn.request(@conn_name, "request.test.request.v1", 100, store_name: @store_name)
      end)
    end

    test "error if reply event doesn't match schema" do
      pid =
        spawn_reply_process("100", fn reply_to, _return_value ->
          Gnat.pub(@conn_name, reply_to, "100", headers: [])
        end)

      assert_raise(Polyn.ValidationException, fn ->
        Polyn.request(@conn_name, "request.test.request.v1", "foo", store_name: @store_name)
      end)

      Process.exit(pid, :kill)
    end
  end

  defp get_message do
    receive do
      {:msg, msg} ->
        msg
    after
      100 ->
        raise "no message"
    end
  end

  defp decode_message(msg), do: Jason.decode!(msg.body)

  # trace_id and span_id get encoded so there's not a good way to test that they match
  defp has_traceparent_header?(headers) do
    Enum.any?(headers, fn {key, _value} -> key == "traceparent" end)
  end

  defp spawn_reply_process(return_value, reply_func \\ &default_reply/2) do
    spawn_link(fn ->
      Gnat.sub(@conn_name, self(), "request.test.request.v1")

      receive do
        {:msg, %{topic: "request.test.request.v1", reply_to: reply_to}} ->
          reply_func.(reply_to, return_value)
      end
    end)
  end

  defp default_reply(reply_to, return_value) do
    Polyn.reply(@conn_name, reply_to, "request.test.response.v1", return_value,
      store_name: @store_name
    )
  end
end
