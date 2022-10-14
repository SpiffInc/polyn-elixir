defmodule Polyn.TracingCase do
  # This module makes testing tracing easier
  # @see https://opentelemetry.io/docs/instrumentation/erlang/testing/
  @moduledoc false

  use ExUnit.CaseTemplate

  # Use Record module to extract fields of the Span record from the opentelemetry dependency.
  require Record

  @span_fields Record.extract(:span, from: "deps/opentelemetry/include/otel_span.hrl")
  @attribute_fields Record.extract(:attributes, from: "deps/opentelemetry/src/otel_attributes.erl")
  @event_fields Record.extract(:event, from: "deps/opentelemetry/include/otel_span.hrl")
  @events_fields Record.extract(:events,
                   from: "deps/opentelemetry/src/otel_events.erl",
                   # allow the `events` record to find the `event` record
                   includes: ["deps/opentelemetry/include"]
                 )

  # Define macros for `Span` including a `span` function for generating expected span structure.
  Record.defrecord(:span_record, :span, @span_fields)
  Record.defrecord(:event_record, :event, @event_fields)
  Record.defrecord(:events_record, :events, @events_fields)
  Record.defrecord(:attributes_record, :attributes, @attribute_fields)

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

  defp expected_span_attributes(attrs) when is_list(attrs) do
    # https://hexdocs.pm/opentelemetry/readme.html#span-limits
    attribute_limit = 128
    value_length_limit = :infinity
    :otel_attributes.new(attrs, attribute_limit, value_length_limit)
  end

  @spec span_attributes(dest :: binary(), id :: binary(), payload :: binary()) ::
          :otel_attributes.t()
  def span_attributes(dest, id, payload) do
    expected_span_attributes([
      {"messaging.system", "NATS"},
      {"messaging.destination", dest},
      {"messaging.protocol", "Polyn"},
      {"messaging.url", "127.0.0.1"},
      {"messaging.message_id", id},
      {"messaging.message_payload_size_bytes", byte_size(payload)}
    ])
  end

  @spec get_events(span :: tuple()) :: keyword()
  def get_events(span_record(events: events_record(list: list))) do
    # Function signature using macro magicalness to pattern match out the list from the records
    Enum.map(list, fn record ->
      event = event_record(record)
      Keyword.put(event, :attributes, attributes_record(event[:attributes])[:map])
    end)
  end
end
