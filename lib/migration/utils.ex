defmodule Polyn.Migration.Utils do
  @moduledoc false

  @doc """
  The conventional file path to the migrations directory
  """
  def migrations_dir do
    Path.join(file().cwd!(), "/priv/polyn/migrations")
  end

  defp file do
    Application.get_env(:polyn, :file, File)
  end
end
