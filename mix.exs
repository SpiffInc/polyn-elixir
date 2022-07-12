defmodule Polyn.MixProject do
  use Mix.Project

  def version, do: "0.1.0"

  def project do
    [
      app: :polyn,
      version: version(),
      elixir: "~> 1.12",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      name: "Polyn",
      source_url: "https://github.com/SpiffInc/polyn-elixir",
      docs: [extras: ["README.md"]]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.0", optional: true},
      {:ex_json_schema, "~> 0.9.1"},
      {:jason, "~> 1.2"},
      {:uuid, "~> 1.1"},
      {:mox, "~> 1.0", only: :test},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:jetstream, git: "https://github.com/mmmries/jetstream"},
      {:ex_doc, "~> 0.28", only: :dev, runtime: false}
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to create, migrate and run the seeds file at once:
  #
  #     $ mix ecto.setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      lint: ["credo --strict"]
    ]
  end
end
