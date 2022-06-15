defmodule OffBroadway.Polyn.Transformer do
  @moduledoc """
  A Broadway [transformer](https://hexdocs.pm/broadway/Broadway.html#start_link/2-producers-options) that
  you can use with `OffBroadway.Jetstream.Producer` to validate and transform NATS server messages into
  valid Polyn Events that conform to your schemas.

  Uses the `OffBroadway` namespace as recommended in the `Broadway` [docs](https://hexdocs.pm/broadway/introduction.html#non-official-off-broadway-producers)
  """

  alias Broadway.Message
  alias Polyn.Serializers.JSON

  def transform(%Message{data: data} = message, opts) do
    {conn, opts} = Keyword.pop!(opts, :connection_name)

    case JSON.deserialize(data, conn, opts) do
      {:ok, event} ->
        Message.update_data(message, fn _data -> event end)

      {:error, error} ->
        raise Polyn.ValidationException, error
    end
  end
end
