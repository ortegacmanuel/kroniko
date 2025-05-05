require 'json'
require 'fileutils'
require 'securerandom'
require 'thread'

class EventStore
  def initialize(base_dir = 'event_store')
    @base_dir = base_dir
    @events_dir = File.join(@base_dir, 'events')
    @index_dir = File.join(@base_dir, 'index')
    @subscribers = []
    @async_queue = Queue.new

    FileUtils.mkdir_p(@events_dir)
    FileUtils.mkdir_p(@index_dir)

    start_async_dispatcher
  end

  def write(event)
    event_id = "#{Time.now.to_f.round(6)}-#{SecureRandom.uuid}"
    full_event = event.merge("id" => event_id)

    File.write(File.join(@events_dir, "#{event_id}.json"), JSON.pretty_generate(full_event))

    index_event(full_event)
    dispatch(full_event)

    full_event
  end

  def read(match: {})
    return [] if match.empty?

    matched_id_sets = []

    match.each do |key, value|
      index_subdir = File.join(@index_dir, key.to_s)
      next unless Dir.exist?(index_subdir)

      matching_files =
        if value.is_a?(Regexp)
          Dir.glob(File.join(index_subdir, '*.jsonl')).select do |file|
            decoded = File.basename(file, '.jsonl')
            decoded.match?(value)
          end
        else
          file = File.join(index_subdir, value.to_s + '.jsonl')
          File.exist?(file) ? [file] : []
        end

      ids = matching_files.flat_map { |file| File.readlines(file).map(&:strip) }.uniq
      matched_id_sets << ids
    end

    return [] if matched_id_sets.empty? || matched_id_sets.any?(&:empty?)

    matched_ids = matched_id_sets.reduce(&:&)

    matched_ids.map do |id|
      path = File.join(@events_dir, "#{id}.json")
      File.exist?(path) ? JSON.parse(File.read(path)) : nil
    end.compact
  end

  def subscribe(&block)
    @subscribers << block
  end

  def subscribe_async(&block)
    @subscribers << proc { |event| @async_queue << [block, event] }
  end

  private

  def index_event(event)
    event.each do |key, value|
      next if key == "id"

      path = File.join(@index_dir, key.to_s, value.to_s + '.jsonl')
      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'a') { |f| f.puts(event["id"]) }
    end
  end

  def dispatch(event)
    @subscribers.each { |subscriber| subscriber.call(event) }
  end

  def start_async_dispatcher
    Thread.new do
      loop do
        subscriber, event = @async_queue.pop
        subscriber.call(event)
      end
    end
  end
end