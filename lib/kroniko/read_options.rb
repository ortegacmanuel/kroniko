module Kroniko
  class ReadOptions
    attr_reader :from, :backwards

    def initialize(from: nil, backwards: false)
      @from = from
      @backwards = backwards
    end
  end
end