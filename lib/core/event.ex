defmodule Polyn.Event do
  @moduledoc """
  The Event structure used throughout Polyn.


  """
  defstruct id: nil,
            specversion: nil,
            type: nil,
            data: nil,
            dataschema: nil,
            source: nil,
            time: nil,
            polyntrace: [],
            polynclient: %{
              lang: "elixir",
              langversion: System.build_info().version,
              version: Polyn.MixProject.version()
            }

  def new(fields) when is_list(fields) do
    fields =
      Keyword.put_new(fields, :id, UUID.uuid4())
      |> Keyword.put_new(:time, DateTime.to_iso8601(DateTime.utc_now()))

    struct!(__MODULE__, fields)
  end
end
