defmodule Polyn.StreamException do
  @moduledoc """
  Error raised when there are problems with a stream or it's not found
  """
  defexception [:message]
end
