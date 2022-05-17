defmodule Polyn.MigrationGenerator do
  @moduledoc false

  require Mix.Generator

  def run(args) do
    args = parse_args(args)
    create_directory(args)
    validate_uniqueness(args)
    generate_file(args)
  end

  defp parse_args(args) do
    %{
      name: Enum.at(args, 0),
      dir: Enum.at(args, 1, migrations_dir())
    }
  end

  def migrations_dir do
    Path.join(File.cwd!(), "/priv/polyn/migrations")
  end

  defp create_directory(%{dir: dir}) do
    File.mkdir_p!(dir)
  end

  defp validate_uniqueness(%{dir: dir, name: name}) do
    fuzzy_path = Path.join(dir, "*_#{base_name(name)}")

    if Path.wildcard(fuzzy_path) != [] do
      Mix.raise(
        "migration can't be created, there is already a migration file with name #{name}."
      )
    end
  end

  defp file_path(%{dir: dir, name: name}) do
    Path.join(dir, file_name(name))
  end

  defp file_name(name) do
    "#{timestamp()}_#{base_name(name)}"
  end

  defp base_name(name) do
    "#{Macro.underscore(name)}.exs"
  end

  # Shamelessly copied from Ecto
  defp timestamp do
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
  end

  defp pad(i) when i < 10, do: <<?0, ?0 + i>>
  defp pad(i), do: to_string(i)

  defp generate_file(%{name: name} = args) do
    file = file_path(args)
    assigns = [mod: migration_module_name(name)]
    Mix.Generator.create_file(file, migration_template(assigns))
    file
  end

  defp migration_module_name(name) do
    Module.concat([Polyn, Migrations, Macro.camelize(name)])
  end

  Mix.Generator.embed_template(:migration, """
    defmodule <%= inspect @mod %> do
      import Polyn.Migration

      def change do
      end
    end
  """)
end
