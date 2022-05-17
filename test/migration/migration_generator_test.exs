defmodule Polyn.MigrationGeneratorTest do
  use ExUnit.Case, async: true

  alias Polyn.FileMock
  alias Polyn.MigrationGenerator
  import Mox

  setup :verify_on_exit!

  test "adds folders if they don't exist" do
    expect_cwd!("my_app")
    expect_mkdir_p!("my_app")
    MigrationGenerator.run("foo")
  end

  test "creates a migration" do
    expect_cwd!("my_app")
    expect_mkdir_p!("my_app")
    MigrationGenerator.run("foo")
  end

  defp expect_mkdir_p!(cwd) do
    path = "#{cwd}/priv/polyn/migrations"
    expect(FileMock, :mkdir_p!, fn ^path -> :ok end)
  end

  defp expect_cwd!(cwd, n \\ 1) do
    expect(FileMock, :cwd!, n, fn -> cwd end)
  end
end
