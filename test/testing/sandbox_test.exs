defmodule Polyn.SandboxTest do
  use ExUnit.Case, async: true

  alias Polyn.Sandbox

  test "starts with no mapped pids" do
    start_supervised!(Sandbox)

    assert Sandbox.state() == %{}
  end

  test "Can setup map a pid to nats pid" do
    start_supervised!(Sandbox)

    assert :ok = Sandbox.setup_test(:foo, :bar)
    assert Sandbox.state() == %{foo: %{nats: :bar}}
  end

  test "Can delete a pid" do
    start_supervised!(Sandbox)

    assert :ok = Sandbox.setup_test(:foo, :bar)
    assert :ok = Sandbox.teardown_test(:foo)
    assert Sandbox.state() == %{}
  end

  describe "allow/2" do
    test "Adds a new allowance" do
      start_supervised!(Sandbox)

      assert :ok = Sandbox.setup_test(:foo, :bar)
      assert :ok = Sandbox.allow(:foo, :other_pid)
      assert Sandbox.get(:other_pid) == %{nats: :bar, allowed_by: [:foo]}
    end

    test "Raises if multiple tests allowing same shared process" do
      Process.flag(:trap_exit, true)
      start_supervised!(Sandbox)

      assert :ok = Sandbox.setup_test(:test1, :nats1)
      assert :ok = Sandbox.setup_test(:test2, :nats2)
      assert :ok = Sandbox.allow(:test1, :other_pid)

      catch_exit(Sandbox.allow(:test2, :other_pid))
    end
  end
end
