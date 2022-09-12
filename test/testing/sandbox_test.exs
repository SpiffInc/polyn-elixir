defmodule Polyn.SandboxTest do
  use ExUnit.Case, async: true

  alias Polyn.Sandbox

  test "starts with no mapped pids" do
    start_supervised!(Sandbox)

    assert Sandbox.state() == %{}
  end

  test "Can map a pid to nats pid" do
    start_supervised!(Sandbox)

    assert Sandbox.allow(:foo, :bar)
    assert Sandbox.state() == %{foo: :bar}
  end

  test "Can delete a pid" do
    start_supervised!(Sandbox)

    assert Sandbox.allow(:foo, :bar)
    assert Sandbox.disallow(:foo)
    assert Sandbox.state() == %{}
  end
end
