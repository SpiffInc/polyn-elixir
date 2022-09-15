defmodule Polyn.SandboxTest do
  use ExUnit.Case, async: true

  alias Polyn.Sandbox

  test "starts with no mapped pids" do
    start_supervised!(Sandbox)

    assert Sandbox.state() == %{async: false, pids: %{}}
  end

  test "setup_test/2 maps a pid to nats pid" do
    start_supervised!(Sandbox)

    assert :ok = Sandbox.setup_test(:foo, :bar)
    assert %{pids: %{foo: %{nats: :bar}}} = Sandbox.state()
  end

  describe "teardown_test/1" do
    test "Can delete a pid" do
      start_supervised!(Sandbox)

      assert :ok = Sandbox.setup_test(:foo, :bar)
      assert :ok = Sandbox.teardown_test(:foo)
      assert %{pids: %{}} = Sandbox.state()
    end

    test "Deletes allowed pids" do
      start_supervised!(Sandbox)

      assert :ok = Sandbox.setup_test(:foo, :bar)
      assert :ok = Sandbox.allow(:foo, :other_pid)
      assert :ok = Sandbox.teardown_test(:foo)
      assert %{pids: %{}} = Sandbox.state()
    end
  end

  describe "allow/2" do
    test "Adds a new allowance" do
      start_supervised!(Sandbox)

      assert :ok = Sandbox.setup_test(:foo, :bar)
      assert :ok = Sandbox.allow(:foo, :other_pid)
      assert Sandbox.get(:other_pid) == :bar
    end

    @tag capture_log: true
    test "Raises if multiple tests allowing same shared process" do
      Process.flag(:trap_exit, true)
      start_supervised!(Sandbox)

      assert :ok = Sandbox.setup_test(:test1, :nats1)
      assert :ok = Sandbox.setup_test(:test2, :nats2)
      assert :ok = Sandbox.allow(:test1, :other_pid)

      catch_exit(Sandbox.allow(:test2, :other_pid))
    end
  end

  describe "async: false" do
    test "Changes async state" do
      start_supervised!(Sandbox)
      assert :ok = Sandbox.set_async_mode(true)
      assert %{async: true} = Sandbox.state()
      assert :ok = Sandbox.set_async_mode(false)
      assert %{async: false} = Sandbox.state()
    end

    test "global access when async false" do
      start_supervised!(Sandbox)
      assert :ok = Sandbox.set_async_mode(false)
      assert :ok = Sandbox.setup_test(:test1, :nats1)
      assert Sandbox.get(:foo) == :nats1
    end
  end
end
