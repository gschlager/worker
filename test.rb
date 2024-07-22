#!/usr/bin/env ruby
# frozen_string_literal: true

def suppress_experimental_ractor_warning
  unless (original_verbose = $VERBOSE).nil?
    $VERBOSE = nil
    Ractor.new {}.take
    $VERBOSE = original_verbose
  end
end

RubyVM::YJIT.enable
suppress_experimental_ractor_warning

# Create a new pipe
reader, writer = IO.pipe

# Create a new Ractor
ractor =
  Ractor.new(reader, writer) do |r, w|
    # Close the writer end in the Ractor
    w.close

    # Read from the pipe
    while (message = r.gets)
      puts "Received message: #{message.chomp}"
    end
  end

# Close the reader end in the main Ractor
reader.close

# Write some messages to the pipe
writer.puts "Hello from main Ractor!"
writer.puts "Another message"

# Close the writer to signal that no more messages will be sent
writer.close

# Wait for the Ractor to finish processing
ractor.take
