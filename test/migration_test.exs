defmodule Polyn.MigrationTest do
  use ExUnit.Case, async: true

  import Polyn.Migration

  test "create a stream" do
    stream_name = "test#{unique()}"
    {:ok, info} = create_stream(name: stream_name, subjects: ["foo"])
    assert info.config.name == stream_name
    :ok = delete_stream(stream_name)
  end

  defp unique do
    System.unique_integer()
  end
end
