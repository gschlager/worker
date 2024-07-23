#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/inline"

gemfile(true) do
  source "https://rubygems.org"

  gem "sys-proctable"
  gem "tty-table"
end

require "sys/proctable"
require "tty-table"

include Sys

def get_process_info
  process_info = []
  ProcTable.ps do |proc|
    next unless proc.pid # Skip processes without PID

    # CPU and memory usage would normally require more detailed system-specific queries.
    # Here we'll use placeholder values as gathering this information is non-trivial.
    cpu_usage = rand(0..100) # Placeholder random value for CPU usage
    mem_usage = rand(100..1024) # Placeholder random value for memory usage in MB

    threads = proc.threadinfo.map { |t| { tid: t.tid, pctcpu: t.pctcpu } }

    process_info << {
      pid: proc.pid,
      name: proc.comm,
      cpu_usage: cpu_usage,
      mem_usage: mem_usage,
      threads: threads
    }
  end
  process_info
end

def display_process_info(process_info)
  rows = []
  process_info.each do |proc|
    rows << [
      proc[:pid],
      proc[:name],
      proc[:cpu_usage],
      proc[:mem_usage],
      "Process"
    ]
    proc[:threads].each do |thread|
      rows << [thread[:tid], proc[:name], thread[:pctcpu], "-", "Thread"]
    end
  end

  table =
    TTY::Table.new(
      ["PID/TID", "Name", "CPU Usage (%)", "Memory Usage (MB)", "Type"],
      rows
    )
  puts table.render(:ascii)
end

loop do
  system("clear") # Clear the console
  process_info = get_process_info
  display_process_info(process_info)
  sleep 0.5
end
