class AppendCondition
  attr_reader :fail_if_events_match, :after

  def initialize(fail_if_events_match:, after: nil)
    @fail_if_events_match = fail_if_events_match
    @after = after
  end
end