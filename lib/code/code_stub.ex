defmodule Polyn.CodeStub do
  @moduledoc false
  @behaviour Polyn.CodeBehaviour

  def compile_file(path) do
    [{Foo, ""}]
  end
end
