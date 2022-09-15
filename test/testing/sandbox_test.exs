defmodule Polyn.SandboxTest do
  use ExUnit.Case, async: false

  alias Polyn.Sandbox

  setup do
    start_supervised!(Sandbox)
    :ok
  end

  test "starts with no mapped pids" do
    assert Sandbox.state() == %{async: false, pids: %{}}
  end

  test "setup_test/2 maps a pid to nats pid" do
    assert :ok = Sandbox.setup_test(:foo, :bar)
    assert %{pids: %{foo: %{nats: :bar}}} = Sandbox.state()
  end

  describe "teardown_test/1" do
    test "Can delete a pid" do
      assert :ok = Sandbox.setup_test(:foo, :bar)
      assert :ok = Sandbox.teardown_test(:foo)
      assert %{pids: %{}} = Sandbox.state()
    end

    test "Deletes allowed pids" do
      assert :ok = Sandbox.setup_test(:foo, :bar)
      assert :ok = Sandbox.allow(:foo, :other_pid)
      assert :ok = Sandbox.teardown_test(:foo)
      assert %{pids: %{}} = Sandbox.state()
    end
  end

  describe "allow/2" do
    test "Adds a new allowance" do
      assert :ok = Sandbox.setup_test(:foo, :bar)
      assert :ok = Sandbox.allow(:foo, :other_pid)
      assert Sandbox.get!(:other_pid) == :bar
    end

    @tag capture_log: true
    test "Raises if multiple tests allowing same shared process" do
      Process.flag(:trap_exit, true)

      assert :ok = Sandbox.setup_test(:test1, :nats1)
      assert :ok = Sandbox.setup_test(:test2, :nats2)
      assert :ok = Sandbox.allow(:test1, :other_pid)

      catch_exit(Sandbox.allow(:test2, :other_pid))
    end

    test "get!/1 raises if no match" do
      assert :ok = Sandbox.set_async_mode(true)
      assert :ok = Sandbox.setup_test(:test1, :nats1)

      %{message: message} =
        assert_raise(Polyn.TestingException, fn ->
          Sandbox.get!(:other_pid)
        end)

      assert message =~
               "There are no MockNats servers\nassociated with process :other_pid"
    end
  end

  describe "async: false" do
    test "Changes async state" do
      assert :ok = Sandbox.set_async_mode(true)
      assert %{async: true} = Sandbox.state()
      assert :ok = Sandbox.set_async_mode(false)
      assert %{async: false} = Sandbox.state()
    end

    test "global access when async false" do
      assert :ok = Sandbox.set_async_mode(false)
      assert :ok = Sandbox.setup_test(:test1, :nats1)
      assert Sandbox.get!(:foo) == :nats1
    end
  end
end
