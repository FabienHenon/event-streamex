defprotocol EventStreamex.Operators.OperatorsProtocol do
  @moduledoc false
  @spec schemas(t()) :: [binary()]
  def schemas(data)

  @spec event_types(t()) :: [atom()]
  def event_types(data)
end
