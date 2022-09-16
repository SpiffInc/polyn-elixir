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

      iex>Polyn.Naming.validate_event_type("user.created")
      :ok

      iex>Polyn.Naming.validate_event_type("user  created")
      {:error, message}
  """
  @spec validate_event_type(name :: binary()) :: :ok | {:error, binary()}
  def validate_event_type(type) do
    if String.match?(type, ~r/^[a-z0-9]+(?:\.[a-z0-9]+)*$/) do
      :ok
    else
      {:error, "Event names must be lowercase, alphanumeric and dot separated"}
    end
  end

  @doc """
  Validate the name of an event, also sometimes called an event `type`.
  Raises if invalid

  ## Examples

      iex>Polyn.Naming.validate_event_type!("user.created")
      :ok

      iex>Polyn.Naming.validate_event_type!("user  created")
      Polyn.ValidationException
  """
  @spec validate_event_type!(name :: binary()) :: :ok
  def validate_event_type!(type) do
    case validate_event_type(type) do
      {:error, reason} ->
        raise Polyn.ValidationException, reason

      success ->
        success
    end
  end

  @doc """
  Validate the source of an event

  ## Examples

      iex>Polyn.Naming.validate_source_name!("user.created")
      :ok

      iex>Polyn.Naming.validate_source_name!("user:created")
      :ok

      iex>Polyn.Naming.validate_source_name!("user  created")
      Polyn.ValidationException
  """
  @spec validate_source_name!(name :: binary()) :: :ok
  def validate_source_name!(name) do
    if String.match?(name, ~r/^[a-z0-9]+(?:(?:\.|\:)[a-z0-9]+)*$/) do
      :ok
    else
      raise Polyn.ValidationException,
            "Event source must be lowercase, alphanumeric and dot/colon separated, got #{name}"
    end
  end

  @doc """
    Create a consumer name from a source and type. Uses the
    configured `:source_root` as the prefix. Will include an
    additional `source` if passed in

    ## Examples

        iex>Polyn.Naming.consumer_name("user.created.v1")
        "user_backend_user_created_v1"

        iex>Polyn.Naming.consumer_name("user.created.v1", "notifications")
        "user_backend_notifications_user_created_v1"
  """

  def consumer_name(type, source \\ nil) do
    validate_event_type!(type)

    type =
      trim_domain_prefix(type)
      |> underscore_name()

    prefix = consumer_prefix(source)
    "#{prefix}_#{type}"
  end

  defp consumer_prefix(nil), do: consumer_prefix()

  defp consumer_prefix(source) do
    root = consumer_prefix()
    validate_source_name!(source)

    "#{root}_#{underscore_name(source)}"
  end

  defp consumer_prefix do
    underscore_name(source_root())
  end

  defp underscore_name(name) do
    String.replace(name, ".", "_")
    |> String.replace(":", "_")
  end

  @doc """
  Lookup the name of a stream for a given event type

  ## Examples

        iex>Polyn.Naming.lookup_stream_name!(:gnat, "user.created.v1")
        "USERS"

        iex>Polyn.Naming.lookup_stream_name!(:gnat, "foo.v1")
        Polyn.StreamException
  """
  def lookup_stream_name!(conn, type) do
    case Polyn.Jetstream.list_streams(conn, subject: type) do
      {:ok, %{streams: [stream]}} ->
        stream

      {:error, error} ->
        raise Polyn.StreamException,
              "Could not find any streams for type #{type}. #{inspect(error)}"

      _ ->
        raise Polyn.StreamException, "Could not find any streams for type #{type}"
    end
  end

  @doc """
  Determine if a given subject matches a subscription pattern
  """
  def subject_matches?(subject, pattern) do
    separator = "."

    pattern_tokens =
      String.split(pattern, separator)
      |> Enum.map_join("\\#{separator}", &build_subject_pattern_part/1)
      |> Regex.compile!()

    String.match?(subject, pattern_tokens)
  end

  defp build_subject_pattern_part("*"), do: "(\\w+)"
  defp build_subject_pattern_part(">"), do: "((\\w+\\.)*\\w)"
  defp build_subject_pattern_part(token), do: token

  defp domain do
    Application.fetch_env!(:polyn, :domain)
  end

  defp source_root do
    Application.fetch_env!(:polyn, :source_root)
  end
end
