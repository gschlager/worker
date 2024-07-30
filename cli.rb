#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/inline"

gemfile(true) do
  source "https://rubygems.org"

  gem "extralite-bundle"
  gem "oj"
end

require_relative "producer"
require_relative "job"
require_relative "writer"
require_relative "worker"

# RubyVM::YJIT.enable

class App
  WORKER_COUNT = [1, Etc.nprocessors - 1].max
  ROW_COUNT = Etc.nprocessors * 1_000
  QUEUE_SIZE = Etc.nprocessors * 100

  def initialize
    @input_queue = SizedQueue.new(QUEUE_SIZE)
    @output_queue = SizedQueue.new(QUEUE_SIZE)
  end

  def start
    start = Time.now

    producer_thread =
      Thread.new do
        Thread.current.name = "producer"
        Producer.new(ROW_COUNT, @input_queue).start
      end

    workers = []
    WORKER_COUNT.times do |index|
      worker = Worker.new(index, @input_queue, @output_queue, Job.new)
      workers << worker
      worker.start
    end

    writer_thread =
      Thread.new do
        Thread.current.name = "writer"
        db_path = File.expand_path("./output/test.db")
        Writer.new(db_path, @output_queue).start
      end

    producer_thread.join
    @input_queue.close
    puts "Producer done"

    workers.each(&:wait)
    @output_queue.close
    puts "Workers done"

    writer_thread.join
    puts "Writer done"

    seconds = Time.now - start
    puts "Done -- #{seconds} seconds"

    db = Extralite::Database.new(File.expand_path("./output/test.db"))
    if (count = db.query_single_splat("SELECT COUNT(*) FROM users")) !=
         ROW_COUNT
      puts "Wrong count: #{count}"
    end
    db.close
  end
end

App.new.start
