#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/inline"

gemfile(true) do
  source "https://rubygems.org"

  gem "extralite-bundle"
  gem "oj"
end

class App
  ROW_COUNT = Etc.nprocessors * 200_000

  def initialize()
    @input_queue = SizedQueue.new(5_000)
    @writer_queue = SizedQueue.new(5_000)
  end

  def start
    producer = Producer.new(ROW_COUNT, @input_queue)
  end
end

App.new.start
