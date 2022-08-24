defmodule PolynTest do
  use Polyn.ConnCase, async: true

  alias Polyn.Event
  alias Polyn.SchemaStore

  @conn_name :producer_gnat
  @moduletag with_gnat: @conn_name

  @store_name "PRODUCER_TEST_SCHEMA_STORE"

  setup do
    :ok = SchemaStore.create_store(@conn_name, name: @store_name)

    on_exit(fn ->
      cleanup()
    end)
  end

  describe "pub/4" do
    test "adds a new event to the server" do
      add_schema("pub.test.event.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      Gnat.sub(@conn_name, self(), "pub.test.event.v1")
      Polyn.pub(@conn_name, "pub.test.event.v1", "foo", store_name: @store_name)

      data = get_message()
      assert data["data"] == "foo"
      assert data["datacontenttype"] == "application/json"
      assert data["source"] == "com:test:user:backend"
      assert data["specversion"] == "1.0.1"
      assert data["type"] == "com.test.pub.test.event.v1"
      assert data["polyntrace"] == []
    end

    test "can include extra `source` info" do
      add_schema("pub.test.event.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      Gnat.sub(@conn_name, self(), "pub.test.event.v1")
      Polyn.pub(@conn_name, "pub.test.event.v1", "foo", store_name: @store_name, source: "orders")

      data = get_message()
      assert data["source"] == "com:test:user:backend:orders"
    end

    test "builds up polyntrace if :triggered_by is supplied" do
      add_schema("pub.test.event.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      Gnat.sub(@conn_name, self(), "pub.test.event.v1")

      trigger_event =
        Event.new(
          type: Event.full_type("user.form.finished.v1"),
          polyntrace: [
            %{
              "id" => Event.new_event_id(),
              "time" => Event.new_timestamp(),
              "type" => Event.full_type("user.entered.site.v1")
            }
          ]
        )

      Polyn.pub(@conn_name, "pub.test.event.v1", "foo",
        store_name: @store_name,
        triggered_by: trigger_event
      )

      data = get_message()

      assert data["polyntrace"] == [
               Enum.at(trigger_event.polyntrace, 0),
               %{
                 "id" => trigger_event.id,
                 "time" => trigger_event.time,
                 "type" => trigger_event.type
               }
             ]
    end

    test "raises if doesn't match schema" do
      add_schema("pub.test.event.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      assert_raise(Polyn.ValidationException, fn ->
        Polyn.pub(@conn_name, "pub.test.event.v1", 100, store_name: @store_name, source: "orders")
      end)
    end

    test "passes through other options" do
      add_schema("pub.test.event.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      Gnat.sub(@conn_name, self(), "pub.test.event.v1")
      Polyn.pub(@conn_name, "pub.test.event.v1", "foo", store_name: @store_name, reply_to: "foo")

      reply_to =
        receive do
          {:msg, %{reply_to: reply_to}} ->
            reply_to
        end

      assert reply_to == "foo"
    end

    test "includes Nats-Msg-Id header" do
      add_schema("pub.test.event.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      Gnat.sub(@conn_name, self(), "pub.test.event.v1")
      Polyn.pub(@conn_name, "pub.test.event.v1", "foo", store_name: @store_name, headers: [{"foo", "bar"}])

      msg =
        receive do
          {:msg, msg} ->
            msg
        end

      data = Jason.decode!(msg.body)
      assert msg.headers == [{"foo", "bar"}, {"nats-msg-id", data["id"]}]
    end
  end

  describe "reply/5" do
    test "replies to a message" do
      add_schema("reply.test.event.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      Gnat.sub(@conn_name, self(), "INBOX.me")
      Polyn.reply(@conn_name, "INBOX.me", "reply.test.event.v1", "foo", store_name: @store_name)

      data = get_message()
      assert data["data"] == "foo"
    end

    test "raises if doesn't match schema" do
      add_schema("reply.test.event.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      assert_raise(Polyn.ValidationException, fn ->
        Polyn.reply(@conn_name, "INBOX.me", "reply.test.event.v1", 100, store_name: @store_name)
      end)
    end
  end

  describe "request/4" do
    test "returned message is an event" do
      add_schema("request.test.request.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      add_schema("request.test.response.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      pid =
        spawn_link(fn ->
          Gnat.sub(@conn_name, self(), "request.test.request.v1")

          receive do
            {:msg, %{topic: "request.test.request.v1", reply_to: reply_to}} ->
              Polyn.reply(@conn_name, reply_to, "request.test.response.v1", "bar",
                store_name: @store_name
              )
          end
        end)

      {:ok, %{body: event}} =
        Polyn.request(@conn_name, "request.test.request.v1", "foo", store_name: @store_name)

      assert event.data == "bar"

      Process.exit(pid, :kill)
    end

    test "error if request event doesn't match schema" do
      add_schema("request.test.request.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      assert_raise(Polyn.ValidationException, fn ->
        Polyn.request(@conn_name, "request.test.request.v1", 100, store_name: @store_name)
      end)
    end

    test "error if reply event doesn't match schema" do
      add_schema("request.test.request.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      add_schema("request.test.response.v1", %{
        "type" => "object",
        "properties" => %{"data" => %{"type" => "string"}}
      })

      pid =
        spawn_link(fn ->
          Gnat.sub(@conn_name, self(), "request.test.request.v1")

          receive do
            {:msg, %{topic: "request.test.request.v1", reply_to: reply_to}} ->
              Gnat.pub(@conn_name, reply_to, "100")
          end
        end)

      assert_raise(Polyn.ValidationException, fn ->
        Polyn.request(@conn_name, "request.test.request.v1", "foo", store_name: @store_name)
      end)

      Process.exit(pid, :kill)
    end
  end

  defp add_schema(type, schema) do
    SchemaStore.save(@conn_name, type, schema, name: @store_name)
  end

  defp get_message do
    receive do
      {:msg, %{body: body}} ->
        Jason.decode!(body)
    after
      100 ->
        raise "no message"
    end
  end

  defp cleanup do
    # Manage connection on our own here, because all supervised processes will be
    # closed by the time `on_exit` runs
    {:ok, pid} = Gnat.start_link()
    SchemaStore.delete_store(pid, name: @store_name)
    Gnat.stop(pid)
  end
end
