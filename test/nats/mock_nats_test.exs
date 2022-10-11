defmodule Polyn.MockNatsTest do
  # Async false because of global sandbox
  use ExUnit.Case, async: false

  alias Polyn.MockNats
  alias Polyn.Sandbox

  setup do
    start_supervised!(Sandbox)
    mock_nats = start_supervised!(Polyn.MockNats)
    Sandbox.setup_test(self(), mock_nats)

    %{nats: mock_nats}
  end

  describe "pub/4" do
    test "stores messages", %{nats: nats} do
      assert :ok = MockNats.pub(:foo, "foo", "bar")

      assert [
               %{
                 gnat: ^nats,
                 topic: "foo",
                 body: "bar",
                 headers: nil,
                 reply_to: nil
               }
             ] = MockNats.get_messages()
    end

    test "includes header and reply_to", %{nats: nats} do
      assert :ok =
               MockNats.pub(:foo, "foo", "bar", headers: [{"key", "value"}], reply_to: "my-inbox")

      assert [
               %{
                 gnat: ^nats,
                 topic: "foo",
                 body: "bar",
                 headers: [{"key", "value"}],
                 reply_to: "my-inbox"
               }
             ] = MockNats.get_messages()
    end

    test "subscribers receive message instanstly", %{nats: _nats} do
      assert {:ok, sid} = MockNats.sub(:foo, self(), "foo")
      assert :ok = MockNats.pub(:foo, "foo", "bar")
      assert_receive({:msg, %{topic: "foo", body: "bar", sid: ^sid}})
    end
  end

  describe "request/4" do
    test "responds to sender" do
      MockNats.sub(:foo, self(), "foo")

      test_pid = self()

      spawn_link(fn ->
        result = MockNats.request(:foo, "foo", "bar")
        send(test_pid, result)
      end)

      receive do
        {:msg, %{topic: "foo", body: "bar", reply_to: reply_to}} ->
          MockNats.pub(:foo, reply_to, "a response")
      end

      assert_receive({:ok, %{body: "a response"}})
    end
  end

  describe "unsub/3" do
    test "removes subscribers" do
      {:ok, sid} = MockNats.sub(:foo, self(), "foo")

      assert MockNats.get_subscribers() == %{
               sid => %{subscriber: self(), sid: sid, subject: "foo"}
             }

      assert :ok = MockNats.unsub(:foo, sid)
      assert MockNats.get_subscribers() == %{}
    end
  end
end
