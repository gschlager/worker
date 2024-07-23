#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/inline"

gemfile(true) do
  source "https://rubygems.org"

  gem "extralite-bundle"
  gem "oj"
  gem "fiber_scheduler"
  gem "io-event"
end

require_relative "worker"

class Job
  def run(data)
    puts data
  end
end

class App
  ROW_COUNT = Etc.nprocessors * 200_000

  def initialize()
    @input_queue = SizedQueue.new(5_000)
    @output_queue = SizedQueue.new(5_000)
  end

  def start
    10.times { |i| @input_queue << "Item #{i}" }
    @input_queue.close

    worker = Worker.new(1, @input_queue, @output_queue, Job.new)
    worker.start
    worker.wait

    puts "Done"
  end
end

App.new.start
