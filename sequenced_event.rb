require_relative 'event'

class SequencedEvent < Event
  attr_reader :position

  def initialize(type:, data:, position:)
    super(type: type, data: data)
    @position = position
  end

  def to_h
    super.merge("position" => position)
  end
end