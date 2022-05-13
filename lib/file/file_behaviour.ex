defmodule Polyn.FileBehaviour do
  # Useful for mocking out the File module
  @moduledoc false

  @callback mkdir!(path :: Path.t()) :: :ok
  @callback ls(path :: Path.t()) :: {:ok, [binary()]} | {:error, File.posix()}
  @callback cwd!() :: binary()
  @callback read(path :: Path.t()) :: {:ok, binary()} | {:error, File.posix()}
  @callback write!(path :: Path.t(), content :: iodata()) ::
              {:ok, binary()} | {:error, File.posix()}
end
