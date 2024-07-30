#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/inline"

gemfile(true) do
  source "https://rubygems.org"

  gem "extralite-bundle"
  gem "oj"
end

require_relative "../multiple_dbs/database"

class App
  ROW_COUNT = Etc.nprocessors * 1_000

  def start
    start = Time.now

    db_path = File.expand_path("./output/test.db")
    db = Database.new(db_path)
    db.open_database(init: true)

    (1..ROW_COUNT).each do |i|
      data = {
        id: i,
        name: "John",
        email: "john@example.com",
        created_at: "2023-12-29T11:10:04Z",
        bio: "a" * 100
      }

      # simulate work
      10.times { |a| 100_000.downto(1) { |b| Math.sqrt(b) * a / 0.2 } }

      db.insert(data)
    end

    db.close

    seconds = Time.now - start
    puts "Done -- #{seconds} seconds"
  end
end

App.new.start
