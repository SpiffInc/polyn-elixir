defmodule Polyn.CloudEvent do
  # Utility functions for working with CloudEvent schemas
  @moduledoc false

  @doc """
  Get the JSON schema from a version number (e.g "1.0.1")
  """
  def json_schema_for_version(version) when is_binary(version) do
    case cloud_events_file("cloud_events_v#{version}.json") do
      {:ok, file} -> Jason.decode!(file)
      error -> error
    end
  end

  defp cloud_events_file(file) do
    # we want to look inside the `priv` directory inside `Polyn` not
    # in the application that depends on Polyn
    Application.app_dir(:polyn, ["priv", "cloud_events", file])
    |> File.read()
  end
end
