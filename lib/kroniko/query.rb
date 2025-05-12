module Kroniko
  class Query
    attr_reader :items

    def initialize(items = [])
      @items = items
    end

    def self.all
      new([])
    end

    def to_match_variants
      return [{}] if @items.empty?
      @items.flat_map(&:to_match_variants)
    end
  end
end