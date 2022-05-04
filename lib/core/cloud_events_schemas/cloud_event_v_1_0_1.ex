# credo:disable-for-next-line
defmodule Polyn.CloudEvent.V_1_0_1 do
  @moduledoc false
  @behaviour Polyn.CloudEvent

  @impl true
  def json_schema do
    %{
      "%id" => "https://raw.githubusercontent.com/cloudevents/spec/v1.0.1/spec.json",
      "$schema" => "http://json-schema.org/draft-07/schema#",
      "definitions" => %{
        "data_base64def" => %{
          "contentEncoding" => "base64",
          "type" => ["string", "null"]
        },
        "datacontenttypedef" => %{"minLength" => 1, "type" => ["string", "null"]},
        "datadef" => %{
          "type" => ["object", "string", "number", "array", "boolean", "null"]
        },
        "dataschemadef" => %{
          "format" => "uri",
          "minLength" => 1,
          "type" => ["string", "null"]
        },
        "iddef" => %{"minLength" => 1, "type" => "string"},
        "sourcedef" => %{
          "format" => "uri-reference",
          "minLength" => 1,
          "type" => "string"
        },
        "specversiondef" => %{"minLength" => 1, "type" => "string"},
        "subjectdef" => %{"minLength" => 1, "type" => ["string", "null"]},
        "timedef" => %{
          "format" => "date-time",
          "minLength" => 1,
          "type" => ["string", "null"]
        },
        "typedef" => %{"minLength" => 1, "type" => "string"}
      },
      "description" => "CloudEvents Specification JSON Schema",
      "properties" => %{
        "data" => %{
          "$ref" => "#/definitions/datadef",
          "description" => "The event payload.",
          "examples" => ["<much wow=\"xml\"/>"]
        },
        "data_base64" => %{
          "$ref" => "#/definitions/data_base64def",
          "description" => "Base64 encoded event payload. Must adhere to RFC4648.",
          "examples" => ["Zm9vYg=="]
        },
        "datacontenttype" => %{
          "$ref" => "#/definitions/datacontenttypedef",
          "description" => "Content type of the data value. Must adhere to RFC 2046 format.",
          "examples" => ["text/xml", "application/json", "image/png", "multipart/form-data"]
        },
        "dataschema" => %{
          "$ref" => "#/definitions/dataschemadef",
          "description" => "Identifies the schema that data adheres to."
        },
        "id" => %{
          "$ref" => "#/definitions/iddef",
          "description" => "Identifies the event.",
          "examples" => ["A234-1234-1234"]
        },
        "source" => %{
          "$ref" => "#/definitions/sourcedef",
          "description" => "Identifies the context in which an event happened.",
          "examples" => [
            "https://github.com/cloudevents",
            "mailto:cncf-wg-serverless@lists.cncf.io",
            "urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66",
            "cloudevents/spec/pull/123",
            "/sensors/tn-1234567/alerts",
            "1-555-123-4567"
          ]
        },
        "specversion" => %{
          "$ref" => "#/definitions/specversiondef",
          "description" => "The version of the CloudEvents specification which the event uses.",
          "examples" => ["1.0"]
        },
        "subject" => %{
          "$ref" => "#/definitions/subjectdef",
          "description" =>
            "Describes the subject of the event in the context of the event producer (identified by source).",
          "examples" => ["mynewfile.jpg"]
        },
        "time" => %{
          "$ref" => "#/definitions/timedef",
          "description" => "Timestamp of when the occurrence happened. Must adhere to RFC 3339.",
          "examples" => ["2018-04-05T17:31:00Z"]
        },
        "type" => %{
          "$ref" => "#/definitions/typedef",
          "description" => "Describes the type of event related to the originating occurrence.",
          "examples" => ["com.github.pull_request.opened", "com.example.object.deleted.v2"]
        }
      },
      "required" => ["id", "source", "specversion", "type"],
      "type" => "object"
    }
  end

  @impl true
  def version, do: "1.0.1"
end
