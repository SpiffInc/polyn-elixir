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
  @spec dot_to_colon(str :: binary()) :: binary()
  def dot_to_colon(str) do
    String.replace(str, ".", ":")
  end

  @doc """
  Convert a colon separated name into a dot separated name

  ## Examples

      iex>Polyn.Naming.colon_to_dot("com:acme:user:created:v1:schema:v1")
      "com.acme.user.created.v1.schema.v1"
  """
  @spec colon_to_dot(str :: binary()) :: binary()
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
  @spec trim_domain_prefix(str :: binary()) :: binary()
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
  @spec version_suffix(str :: binary(), version :: non_neg_integer()) :: binary()
  @spec version_suffix(str :: binary()) :: binary()
  def version_suffix(str, version \\ 1) do
    "#{str}v#{version}"
  end

  @doc """
  Remove the version suffix from a name

  ## Examples

      iex>Polyn.Naming.trim_version_suffix("com.acme.user.created.v1")
      "com.acme.user.created"

      iex>Polyn.Naming.trim_version_suffix("com:acme:user:created:v1")
      "com:acme:user:created"
  """
  @spec trim_version_suffix(str :: binary()) :: binary()
  def trim_version_suffix(str) do
    String.replace(str, ~r/[\.\:]+v\d+/, "")
  end

  @doc """
  Validate the name of an event, also sometimes called an event `type`.

  ## Examples

      iex>Polyn.Naming.validate_event_name("user.created")
      :ok

      iex>Polyn.Naming.validate_event_name("user  created")
      {:error, ["Event names can't have spaces"]}
  """
  @spec validate_event_name(name :: binary()) :: :ok | {:error, binary()}
  def validate_event_name(name) do
    if String.match?(name, ~r/^[a-z0-9]+(?:\.[a-z0-9]+)?$/) do
      :ok
    else
      {:error, "Event names must be lowercase, alphanumeric and dot separated"}
    end
  end

  @spec validate_source_name(name :: binary()) :: :ok | {:error, binary()}
  def validate_source_name(name) do
    if String.match?(name, ~r/^[a-z0-9]+(?:(?:\.|\:)[a-z0-9]+)?$/) do
      :ok
    else
      {:error, "Event source must be lowercase, alphanumeric and dot/colon separated"}
    end
  end

  defp domain do
    Application.fetch_env!(:polyn, :domain)
  end
end
