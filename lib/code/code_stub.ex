defmodule Polyn.CodeStub do
  @moduledoc false
  @behaviour Polyn.CodeBehaviour

  def compile_file(_path) do
    [{Foo, ""}]
  end
end
