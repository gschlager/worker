# frozen_string_literal: true

require "oj"

class Worker
  def initialize(index, input_queue, output_queue, job)
    @index = index
    @input_queue = input_queue
    @output_queue = output_queue
    @job = job

    @mutex = Mutex.new
    @data_processed = ConditionVariable.new
    @threads = []
  end

  def start
    start_fork
    start_input_thread
    start_output_thread
    self
  end

  def wait
    @threads.each(&:join)
  end

  private

  def start_fork
    io_from_parent, parent_writer = IO.pipe
    io_from_child, child_writer = IO.pipe

    @worker_pid =
      Process.fork do
        begin
          Process.setproctitle("worker_process#{@index}")

          parent_writer.close
          io_from_child.close

          Oj.load(io_from_parent) do |data|
            @job.run(data)
            child_writer.write(Oj.dump(data))
          end
        rescue SignalException
          exit(1)
        end
      end

    child_writer.close
    io_from_parent.close

    @writer = parent_writer
    @reader = io_from_child
  end

  def start_input_thread
    @threads << Thread.new do
      Thread.current.name = "worker_#{@index}_input"

      begin
        while (data = @input_queue.pop)
          @writer.write(Oj.dump(data))
          @mutex.synchronize { @data_processed.wait(@mutex) }
        end
      ensure
        @writer.close
        Process.waitpid(@worker_pid)
      end
    end
  end

  def start_output_thread
    @threads << Thread.new do
      Thread.current.name = "worker_#{@index}_output"

      begin
        Oj.load(@reader) do |data|
          @output_queue.push(data)
          @mutex.synchronize { @data_processed.signal }
        end
      ensure
        @reader.close
      end
    end
  end
end
