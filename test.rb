#!/usr/bin/env ruby
# frozen_string_literal: true

require "oj"

# Create a pipe
reader, writer = IO.pipe

# Data to be written to the pipe
data = [
  { name: "Alice", age: 30, city: "Wonderland" },
  { name: "Bob", age: 25, city: "Builderland" }
]

threads = []
mutex = Mutex.new
data_processed = ConditionVariable.new

# Write JSON data to the writer stream in a separate thread
threads << Thread.new do
  data.each do |item|
    Oj.to_stream(writer, item, mode: :custom)
    mutex.synchronize { data_processed.wait(mutex) }
  end
  writer.close
end

parser =
  Oj::Parser.new(
    :usual,
    cache_keys: true,
    missing_class: :raise,
    symbol_keys: true
  )
results = []

threads << Thread.new do
  begin
    while true
      data = +""
      while (buffer = reader.readpartial(4096))
        data << buffer
        break if buffer.length < 4096
      end
      puts parser.parse(buffer)
      mutex.synchronize { data_processed.signal }
    end
  rescue EOFError
    # ignore
  ensure
    reader.close
  end
end

threads.each(&:join)

# Print the parsed results
results.each { |result| puts result.inspect }
