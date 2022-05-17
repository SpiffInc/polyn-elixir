defmodule Polyn.MigrationGenerator do
  @moduledoc false

  def run do
    create_directory()
  end

  defp create_directory do
    file().mkdir_p!(Path.join(file().cwd!(), "priv/polyn/migrations"))
  end

  defp file do
    Application.get_env(:polyn, :file, File)
  end
end
