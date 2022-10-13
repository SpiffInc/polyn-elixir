defmodule Polyn.TracingCase do
  @moduledoc """
  This module makes testing tracing easier
  @see https://opentelemetry.io/docs/instrumentation/erlang/testing/
  """

  use ExUnit.CaseTemplate

  # Use Record module to extract fields of the Span record from the opentelemetry dependency.
  require Record
  @fields Record.extract(:span, from: "deps/opentelemetry/include/otel_span.hrl")
  # Define macros for `Span` including a `span` function for generating expected span structure.
  Record.defrecord(:span, @fields)

  using(_opts) do
    quote do
      import Polyn.TracingCase
    end
  end

  def start_collecting_spans do
    # Set exporter to :otel_exporter_pid, which sends spans
    # to the given process - in this case self() - in the format {:span, span}
    :otel_simple_processor.set_exporter(:otel_exporter_pid, self())
  end

  @spec expected_span_attributes(attrs :: keyword()) :: :otel_attributes.t()
  def expected_span_attributes(attrs) when is_list(attrs) do
    # https://hexdocs.pm/opentelemetry/readme.html#span-limits
    attribute_limit = 128
    value_length_limit = :infinity
    :otel_attributes.new(attrs, attribute_limit, value_length_limit)
  end
end
