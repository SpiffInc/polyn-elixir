defmodule Polyn.MigrationGeneratorTest do
  use ExUnit.Case, async: true

  alias Polyn.MigrationGenerator

  @moduletag :tmp_dir

  test "adds folders if they don't exist", %{tmp_dir: tmp_dir} do
    MigrationGenerator.run(["foo", tmp_dir])
    assert File.dir?(tmp_dir)
  end

  test "creates a migration", %{tmp_dir: tmp_dir} do
    path = MigrationGenerator.run(["my_migration", tmp_dir])
    assert Path.dirname(path) == tmp_dir
    assert Path.basename(path) =~ ~r/^\d{14}_my_migration\.exs$/

    assert_file(path, fn file ->
      assert file =~ "defmodule Polyn.Migrations.MyMigration do"
      assert file =~ "import Polyn.Migration"
      assert file =~ "def change do"
    end)
  end

  test "underscores the filename when generating a migration", %{tmp_dir: tmp_dir} do
    MigrationGenerator.run(["MyMigration", tmp_dir])
    assert [name] = File.ls!(tmp_dir)
    assert name =~ ~r/^\d{14}_my_migration\.exs$/
  end

  test "raises when existing migration exists", %{tmp_dir: tmp_dir} do
    MigrationGenerator.run(["my_migration", tmp_dir])

    assert_raise Mix.Error, ~r"migration can't be created", fn ->
      MigrationGenerator.run(["my_migration", tmp_dir])
    end
  end

  def assert_file(file) do
    assert File.regular?(file), "Expected #{file} to exist, but does not"
  end

  def assert_file(file, callback) when is_function(callback, 1) do
    assert_file(file)
    callback.(File.read!(file))
  end
end
