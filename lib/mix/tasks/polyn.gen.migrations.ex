defmodule Mix.Tasks.Polyn.Gen.Migration do
  @moduledoc """
  Use `mix polyn.gen.migration <name>` to generate a new migration module for your application

  The `<name>` argument should be the snake_cased name representing the change the migration will make.any()

  The generated migration filename will be prefixed with the current timestamp in UTC which is used for versioning and ordering.
  """
  @shortdoc "Generates a new migration file"

  use Mix.Task

  def run([name]) do
    Polyn.MigrationGenerator.run(name)
  end
end
