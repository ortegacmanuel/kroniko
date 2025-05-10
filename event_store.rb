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

  def read(query:)
    match_variants = query.to_match_variants

    # Handle Query.all
    if match_variants == [{}]
      return Dir.glob(File.join(@events_dir, '*.json')).map do |path|
        JSON.parse(File.read(path))
      end
    end

    # Step 1: Resolve all index file paths needed (per key/value or regex)
    index_cache = {}
    match_variants.each do |match|
      match.each do |key, value|
        index_cache[key.to_s] ||= resolve_index_files(key.to_s, value)
      end
    end

    # Step 2: Read each index file once
    file_id_map = {}
    index_cache.values.flatten.uniq.each do |path|
      file_id_map[path] = File.readlines(path, chomp: true) if File.exist?(path)
    end

    # Step 3: For each match variant, collect IDs using intersect (AND logic)
    matched_ids = match_variants.flat_map do |match|
      id_sets = match.map do |key, value|
        files = resolve_index_files(key.to_s, value)
        files.flat_map { |file| file_id_map[file] || [] }.uniq
      end
      id_sets.empty? || id_sets.any?(&:empty?) ? [] : id_sets.reduce(&:&)
    end.uniq

    # Step 4: Load events
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

  def resolve_index_files(key, value)
    index_subdir = File.join(@index_dir, key)
    return [] unless Dir.exist?(index_subdir)

    if value.is_a?(Regexp)
      Dir.glob(File.join(index_subdir, "*.jsonl")).select do |file|
        File.basename(file, ".jsonl").match?(value)
      end
    else
      file = File.join(index_subdir, value.to_s + ".jsonl")
      File.exist?(file) ? [file] : []
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