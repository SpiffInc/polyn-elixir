defmodule Mix.Tasks.Polyn.Migrate do
  @moduledoc """
  Use `mix polyn.migrate` to make configuration changes to your NATS server.
  """
  @shortdoc "Runs migrations to make modifications to your NATS Server"

  use Mix.Task

  def run(_) do
    Polyn.Migrator.run("foo")
  end
end
