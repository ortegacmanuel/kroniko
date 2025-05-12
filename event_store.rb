require 'json'
require 'fileutils'
require 'securerandom'
require 'thread'

require_relative 'event'
require_relative 'sequenced_event'
require_relative 'lock_timeout_error'
require_relative 'append_condition_failed'

class EventStore
  MAX_RETRIES = 5
  RETRY_WAIT = 0.05

  def initialize(base_dir = 'event_store')
    @base_dir = base_dir
    @events_dir = File.join(@base_dir, 'events')
    @index_dir = File.join(@base_dir, 'index')
    @log_file = File.join(@base_dir, 'log', 'append.log')
    @locks_dir = File.join(@base_dir, 'locks')

    @subscribers = []
    @async_queue = Queue.new

    FileUtils.mkdir_p(@events_dir)
    FileUtils.mkdir_p(@index_dir)
    FileUtils.mkdir_p(File.dirname(@log_file))
    FileUtils.mkdir_p(@locks_dir)

    start_async_dispatcher
  end

  def write(events:, condition: nil)
    retries = 0
    begin
      if condition
        with_query_lock(condition.fail_if_events_match) do
          check_append_condition!(condition)
          return _write(events)
        end
      else
        return _write(events)
      end
    rescue AppendConditionFailed => e
      puts "[append_condition] failed for query: #{condition.fail_if_events_match.inspect}"
      raise e
    rescue LockTimeoutError
      puts "[lock_timeout] retry=#{retries}"
      retries += 1
      sleep_time = RETRY_WAIT * (2 ** retries)
      sleep(sleep_time)
      retry if retries < MAX_RETRIES
      raise "Failed to acquire lock after #{MAX_RETRIES} attempts"
    end
  end

  def _write(events)
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

    if match_variants == [{}]
      events = File.readlines(@log_file, chomp: true).map do |event_id|
        path = File.join(@events_dir, "#{event_id}.json")
        next unless File.exist?(path)
        event = JSON.parse(File.read(path))
        SequencedEvent.new(
          type: event["type"],
          data: event["data"],
          position: position_for(event_id)
        )
      end.compact
    else
      index_files = resolve_index_files_for_variants(match_variants)
      file_id_map = read_index_files(index_files)
      matched_ids = resolve_id_sets(match_variants, file_id_map)

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

  def with_query_lock(query)
    lock_material = query.to_match_variants.map do |variant|
      variant.map { |k, v| [k.to_s, v.is_a?(Regexp) ? v.source : v.to_s] }.sort
    end.sort

    lock_key = Digest::SHA256.hexdigest(Marshal.dump(lock_material))[0..16]
    lock_path = File.join(@locks_dir, lock_key + ".lock")

    File.open(lock_path, 'w') do |f|
      locked = false
      attempt = 0
      started_at = Time.now

      5.times do
        attempt += 1
        locked = f.flock(File::LOCK_EX | File::LOCK_NB)
        break if locked
        sleep 0.01
      end

      duration = ((Time.now - started_at) * 1000).round(2)
      puts "[lock] acquired=#{locked} key=#{lock_key} attempts=#{attempt} duration_ms=#{duration}"

      raise LockTimeoutError unless locked
      yield
    ensure
      f.flock(File::LOCK_UN) if locked
    end
  end  

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

  def check_append_condition!(condition)
    if any_matching_event_ids?(
      query: condition.fail_if_events_match,
      after: condition.after
    )
      raise AppendConditionFailed, "AppendCondition failed: matching events already exist"
    end
  end

  def any_matching_event_ids?(query:, after: nil)
    match_variants = query.to_match_variants
    index_files = resolve_index_files_for_variants(match_variants)
    file_id_map = read_index_files(index_files)
    matched_ids = resolve_id_sets(match_variants, file_id_map)

    after ? matched_ids.any? { |id| position_for(id) > after } : !matched_ids.empty?
  end  

  def resolve_index_files_for_variants(match_variants)
    match_variants.flat_map do |match|
      match.flat_map { |key, value| resolve_index_files(key.to_s, value) }
    end.uniq
  end

  def resolve_id_sets(match_variants, file_id_map)
    match_variants.map do |match|
      id_sets = match.map do |key, value|
        files = resolve_index_files(key.to_s, value)
        files.flat_map { |file| file_id_map[file] || [] }.uniq
      end
      id_sets.empty? || id_sets.any?(&:empty?) ? [] : id_sets.reduce(&:&)
    end.flatten.uniq
  end

  def read_index_files(index_files)
    index_files.each_with_object({}) do |path, map|
      map[path] = File.readlines(path, chomp: true) if File.exist?(path)
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