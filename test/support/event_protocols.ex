defmodule EventProtocols do
  defmodule ScopedComment do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "comments",
      module_name: "TestEvents",
      extra_channels: [%{scopes: [post_id: "posts"]}],
      scheduler: :test_scheduler

    defstruct([:id, :comment, :post_id])
  end

  defmodule OperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "operators",
      module_name: "TestOperators"

    defstruct([:id, :msg])
  end

  defmodule RaisedOperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "raised_operators",
      module_name: "TestOperators"

    defstruct([:id, :msg])
  end

  defmodule FailedOperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "failed_operators",
      module_name: "TestOperators"

    defstruct([:id, :msg])
  end

  defmodule BadOperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "bad_operators",
      module_name: "TestOperators"

    defstruct([:id, :msg])
  end

  defmodule FilteredOperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "filtered_operators",
      module_name: "TestOperators"

    defstruct([:id, :msg])
  end

  defmodule LongOperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "long_operators",
      module_name: "TestOperators"

    defstruct([:id, :msg])
  end

  defmodule BaseEntity1OperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "base_entities1",
      module_name: "TestOperators"

    defstruct([:id, :base_entity2_id])
  end

  defmodule BaseEntity2OperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "base_entities2",
      module_name: "TestOperators"

    defstruct([:id, :title])
  end

  defmodule BaseEntity3OperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "base_entities3",
      module_name: "TestOperators"

    defstruct([:id, :base_entity4_id])
  end

  defmodule BaseEntity4OperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "base_entities4",
      module_name: "TestOperators"

    defstruct([:id, :title])
  end

  defmodule MergedEntityOperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "merged_entities",
      module_name: "TestOperators"

    defstruct([:id, :title, :count])
  end

  defmodule ComplexEntityOperatorEvent do
    use EventStreamex.Events,
      name: :event_streamex,
      schema: "complex_entities",
      module_name: "TestOperators"

    defstruct([:id, :title, :count])
  end
end
