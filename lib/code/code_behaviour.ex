defmodule Polyn.CodeBehaviour do
  # a behaviour to use for Mocking the `Elixir.Code` module
  @moduledoc false
  @callback compile_file(Path.t()) :: [{module(), binary()}]
end
