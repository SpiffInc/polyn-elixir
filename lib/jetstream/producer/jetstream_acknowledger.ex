with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule Polyn.Jetstream.Acknowledger do
    # Real calls to Offbroadway.Jetstream.Acknowledger
    # This exists to ensure we don't mock things we don't "own"
    @moduledoc false

    @behaviour Broadway.Acknowledger

    @impl Broadway.Acknowledger
    defdelegate ack(ack_ref, successful, failed), to: OffBroadway.Jetstream.Acknowledger

    @impl Broadway.Acknowledger
    defdelegate configure(ack_ref, ack_data, options), to: OffBroadway.Jetstream.Acknowledger
  end
end
