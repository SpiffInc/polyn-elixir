defmodule Polyn.Naming do
  @moduledoc """
  Utilities for working with Polyn naming conventions
  """

  @doc """
  Convert a dot separated name into a colon separated name

  ## Examples

      iex>Polyn.Naming.dot_to_colon("com.acme.user.created.v1.schema.v1")
      "com:acme:user:created:v1:schema:v1"
  """
  def dot_to_colon(str) do
    String.replace(str, ".", ":")
  end

  @doc """
  Convert a colon separated name into a dot separated name

  ## Examples

      iex>Polyn.Naming.colon_to_dot("com:acme:user:created:v1:schema:v1")
      "com.acme.user.created.v1.schema.v1"
  """
  def colon_to_dot(str) do
    String.replace(str, ":", ".")
  end

  @doc """
  Remove the `:domain` prefix from a name

  ## Examples

      iex>Polyn.Naming.trim_domain_prefix("com:acme:user:created:v1:schema:v1")
      "user:created:v1:schema:v1"

      iex>Polyn.Naming.trim_domain_prefix("com.acme.user.created.v1.schema.v1")
      "user.created.v1.schema.v1"
  """
  def trim_domain_prefix(str) do
    String.replace(str, "#{domain()}.", "", global: false)
    |> String.replace("#{dot_to_colon(domain())}:", "", global: false)
  end

  @doc """
  Give a version number to a name

  ## Examples

      iex>Polyn.Naming.version_suffix("com:acme:user:created:")
      "com:acme:user:created:v1"

      iex>Polyn.Naming.version_suffix("com.acme.user.created.", 2)
      "com.acme.user.created.v2"
  """
  def version_suffix(str, version \\ 1) do
    "#{str}v#{version}"
  end

  defp domain do
    Application.fetch_env!(:polyn, :domain)
  end
end
