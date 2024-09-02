defprotocol EventStreamex.EventsProtocol do
  @moduledoc false
  @spec table_name(t()) :: binary()
  def table_name(data)

  @spec module(t()) :: atom()
  def module(data)
end
