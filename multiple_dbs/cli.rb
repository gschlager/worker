#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/inline"

gemfile(true) do
  source "https://rubygems.org"

  gem "extralite-bundle"
  gem "msgpack"
end

require_relative "database"
require_relative "producer"
require_relative "job"
require_relative "worker"

# RubyVM::YJIT.enable

class App
  WORKER_COUNT = [1, Etc.nprocessors - 1].max
  ROW_COUNT = Etc.nprocessors * 1_000

  def initialize
    @input_queue = SizedQueue.new(5_000)
    @output_queue = SizedQueue.new(5_000)
  end

  def start
    start = Time.now

    db_path = File.expand_path("./output/test.db")
    db = Database.new(db_path)
    db.open_database(init: true)
    db.close

    producer_thread =
      Thread.new do
        Thread.current.name = "producer"
        Producer.new(ROW_COUNT, @input_queue).start
      end

    status_thread =
      Thread.new do
        count = 0

        while (stats = @output_queue.pop)
          count += 1
          print "\r#{count}"
        end

        puts ""
        @output_queue.close
      end

    source_db_paths = []
    workers = []
    WORKER_COUNT.times do |index|
      temp_db_path = File.expand_path("./output/temp/#{index}/temp.db")
      source_db_paths << temp_db_path
      worker =
        Worker.new(index, @input_queue, @output_queue, Job.new, temp_db_path)
      workers << worker
      worker.start
    end

    producer_thread.join
    @input_queue.close
    puts "Producer done"

    workers.each(&:wait)
    @output_queue.close
    puts "Workers done"

    status_thread.join
    puts "Status thread done"

    db = Database.new(db_path)
    db.open_database(init: false)
    db.copy_from(source_db_paths)
    if (count = db.db.query_single_splat("SELECT COUNT(*) FROM users")) !=
         ROW_COUNT
      puts "Wrong count: #{count}"
    end
    db.close

    seconds = Time.now - start
    puts "Done -- #{seconds} seconds"
  end
end

App.new.start
