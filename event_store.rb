require 'json'
require 'fileutils'
require 'securerandom'
require 'thread'

require_relative 'event'
require_relative 'sequenced_event'

require 'debug'

class EventStore
  def initialize(base_dir = 'event_store')
    @base_dir = base_dir
    @events_dir = File.join(@base_dir, 'events')
    @index_dir = File.join(@base_dir, 'index')
    @log_file = File.join(@base_dir, 'log', 'append.log')
    @subscribers = []
    @async_queue = Queue.new

    FileUtils.mkdir_p(@events_dir)
    FileUtils.mkdir_p(@index_dir)
    FileUtils.mkdir_p(File.dirname(@log_file))

    start_async_dispatcher
  end

  def write(events:)
    raise ArgumentError, "events must be an array of Event" unless events.is_a?(Array)

    events.map do |event|
      raise ArgumentError, "each item must be an Event" unless event.is_a?(Event)
      write_single(event)
    end
  end

  def write_single(event)
    event_id = "#{Time.now.to_f.round(6)}-#{SecureRandom.uuid}"
    event_path = File.join(@events_dir, "#{event_id}.json")

    full_event = event.to_h.merge("id" => event_id)

    File.write(event_path, JSON.pretty_generate(full_event))
    File.open(@log_file, 'a') { |f| f.puts(event_id) }

    index_event(full_event)
    dispatch(full_event)

    full_event
  end

  def position_for(event_id)
    return nil unless File.exist?(@log_file)

    IO.popen(["grep", "-n", "^#{event_id}$", @log_file]) do |io|
      line = io.gets
      return nil unless line
      line.split(":").first.to_i
    end
  end

  def read(query:, options: nil)
    match_variants = query.to_match_variants

    # Handle Query.all
    if match_variants == [{}]
      events = query_all
    else ## Matching query
      # Step 1: Resolve all index file paths needed (per match variant)
      all_index_files = match_variants.flat_map do |match|
        match.flat_map { |key, value| resolve_index_files(key.to_s, value) }
      end.uniq

      # Step 2: Read each index file once
      file_id_map = {}
      all_index_files.each do |path|
        file_id_map[path] = File.readlines(path, chomp: true) if File.exist?(path)
      end

      # Step 3: Collect matched ID sets per match variant (OR logic)
      match_sets = match_variants.map do |match|
        id_sets = match.map do |key, value|
          files = resolve_index_files(key.to_s, value)
          files.flat_map { |file| file_id_map[file] || [] }.uniq
        end
        id_sets.empty? || id_sets.any?(&:empty?) ? [] : id_sets.reduce(&:&)
      end

      matched_ids = match_sets.flatten.uniq

      # Step 4: Load and return events ordered by position
      events = matched_ids.map do |id|
        path = File.join(@events_dir, "#{id}.json")
        next unless File.exist?(path)

        raw = JSON.parse(File.read(path))
        SequencedEvent.new(
          type: raw["type"],
          data: raw["data"],
          position: position_for(id)
        )
      end.compact
    end

    # Apply ReadOptions (if any)
    events = events.sort_by(&:position)
    if options&.backwards
      events.reverse!
      events = events.select { |e| e.position <= options.from } if options.from
    elsif options&.from
      events = events.select { |e| e.position >= options.from }
    end

    events
  end

  def subscribe(&block)
    @subscribers << block
  end

  def subscribe_async(&block)
    @subscribers << proc { |event| @async_queue << [block, event] }
  end

  private

  def index_event(event)
    path_type = File.join(@index_dir, "type", event["type"] + ".jsonl")
    FileUtils.mkdir_p(File.dirname(path_type))
    File.open(path_type, 'a') { |f| f.puts(event["id"]) }

    if event["data"].is_a?(Hash)
      event["data"].each do |key, value|
        path = File.join(@index_dir, "data.#{key}", value.to_s + '.jsonl')
        FileUtils.mkdir_p(File.dirname(path))
        File.open(path, 'a') { |f| f.puts(event["id"]) }
      end
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

  def query_all
    return File.readlines(@log_file, chomp: true).map do |event_id|
      path = File.join(@events_dir, "#{event_id}.json")
      next unless File.exist?(path)

      raw = JSON.parse(File.read(path))
      SequencedEvent.new(
        type: raw["type"],
        data: raw["data"],
        position: position_for(event_id)
      )
    end.compact
  end
end