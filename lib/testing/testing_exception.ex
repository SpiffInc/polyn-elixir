defmodule Polyn.TestingException do
  @moduledoc """
  Error raised when test things are not setup right
  """
  defexception [:message]
end
