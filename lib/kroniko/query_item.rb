module Kroniko
  class QueryItem
    attr_reader :types, :properties

    def initialize(types: [], properties: {})
      @types = Array(types).map(&:to_s)
      @properties = properties.transform_keys(&:to_s)
    end

    def to_match_variants
      base = {}

      unless @types.empty?
        pattern = Regexp.new("^(#{@types.join('|')})$", Regexp::IGNORECASE)
        base["type"] = pattern
      end

      @properties.each do |k, v|
        base["data.#{k}"] = v
      end

      [base]
    end
  end
end