defmodule Polyn.MigrationGenerator do
  @moduledoc false

  require Mix.Generator
  import Polyn.Migration.Utils, only: [migrations_dir: 0]

  def run([name]) do
    create_directory()
    validate_uniqueness(name)
    generate_file(name)
  end

  defp create_directory do
    file().mkdir_p!(migrations_dir())
  end

  defp validate_uniqueness(name) do
    fuzzy_path = Path.join(migrations_dir(), "*_#{base_name(name)}")

    if Path.wildcard(fuzzy_path) != [] do
      Mix.raise(
        "migration can't be created, there is already a migration file with name #{name}."
      )
    end
  end

  defp file_path(name) do
    Path.join(migrations_dir(), file_name(name))
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

  defp file do
    Application.get_env(:polyn, :file, File)
  end

  defp generate_file(name) do
    assigns = [mod: migration_module_name(name)]
    Mix.Generator.create_file(file_path(name), migration_template(assigns))
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
