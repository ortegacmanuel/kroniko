module Kroniko
  class Event
    attr_reader :type, :data

    def initialize(data:, type: nil)
      @data = data
      @type = type || self.class.name
    end

    def to_h
      { "type" => type, "data" => data }
    end
  end
end