defmodule Polyn.SchemaException do
  @moduledoc """
  Error raised when schemas are not found where they are expected to be
  """
  defexception [:message]
end
