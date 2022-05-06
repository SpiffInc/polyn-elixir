defmodule Polyn.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      %{
        id: Gnat.ConnectionSupervisor,
        start: {
          Gnat.ConnectionSupervisor,
          :start_link,
          [Application.fetch_env!(:polyn, :nats), [name: :polyn_connection_supervisor]]
        }
      }
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Polyn.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
