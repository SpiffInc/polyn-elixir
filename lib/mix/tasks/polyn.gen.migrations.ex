defmodule Mix.Tasks.Polyn.Gen.Migration do
  @moduledoc """
  Use `mix polyn.gen.migration` to generate a new migration module for your application
  """
  @shortdoc "Generates a new migration file"

  use Mix.Task

  def run(_) do
    Polyn.MigrationGenerator.run()
  end
end
