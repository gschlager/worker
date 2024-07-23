#!/usr/bin/env ruby
# frozen_string_literal: true

pid =
  fork do
    Process.setproctitle("test_process")
    sleep(60)
  end
Process.waitpid(pid)
