defmodule Polyn.CloudEvent do
  # Behaviour for defining CloudEvent versions and some
  # utility functions for working with them
  @moduledoc false
  @callback json_schema() :: map()
  @callback version() :: String.t()

  @doc """
  Get the JSON schema from a version number (e.g "1.0.1")
  """
  def json_schema_for_version(version) when is_binary(version) do
    apply(module_for_version(version), :json_schema, [])
  end

  @doc """
  Find the module that corresponds to a CloudEvent version
  """
  def module_for_version(version) when is_binary(version) do
    suffix = "V_" <> String.replace(version, ".", "_")

    String.to_existing_atom("Elixir.Polyn.CloudEvent.#{suffix}")
  end
end
