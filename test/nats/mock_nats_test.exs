defmodule Polyn.MockNatsTest do
  use ExUnit.Case, async: true

  alias Polyn.MockNats

  describe "pub/4" do
    test "stores messages" do
      nats = start_supervised!(MockNats)
      assert :ok = MockNats.pub(nats, "foo", "bar")

      assert [%{gnat: ^nats, topic: "foo", body: "bar", headers: nil, reply_to: nil}] =
               MockNats.get_messages(nats)
    end

    test "includes header and reply_to" do
      nats = start_supervised!(MockNats)

      assert :ok =
               MockNats.pub(nats, "foo", "bar", headers: [{"key", "value"}], reply_to: "my-inbox")

      assert [
               %{
                 gnat: ^nats,
                 topic: "foo",
                 body: "bar",
                 headers: [{"key", "value"}],
                 reply_to: "my-inbox"
               }
             ] = MockNats.get_messages(nats)
    end

    test "subscribers receive message instanstly" do
      nats = start_supervised!(MockNats)
      assert {:ok, sid} = MockNats.sub(nats, self(), "foo")
      assert :ok = MockNats.pub(nats, "foo", "bar")
      assert_receive({:msg, %{topic: "foo", body: "bar", sid: ^sid}})
    end
  end
end
