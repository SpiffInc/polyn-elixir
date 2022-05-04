defmodule Polyn.FileBehaviour do
  @moduledoc false

  @callback mkdir!(path :: Path.t()) :: :ok
  @callback cwd!() :: binary()
  @callback read(path :: Path.t()) :: {:ok, binary()} | {:error, File.posix()}
  @callback write!(path :: Path.t(), content :: iodata()) ::
              {:ok, binary()} | {:error, File.posix()}
end
