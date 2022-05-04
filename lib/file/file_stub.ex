defmodule Polyn.FileStub do
  @behaviour Polyn.FileBehaviour

  def mkdir!(_path) do
    :ok
  end

  def cwd! do
    "foo"
  end

  def read(_path) do
    {:ok, "foo"}
  end

  def write!(_path, content) do
    content
  end
end
