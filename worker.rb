# frozen_string_literal: true

require "oj"

class Worker
  def initialize(index, input_queue, output_queue, job)
    @index = index
    @input_queue = input_queue
    @output_queue = output_queue
    @job = job
  end

  def start
    start_fork
    start_input_fiber
    start_output_fiber
  end

  def wait
    Fiber.yield while @input_fiber.alive? || @output_fiber.alive?
    @writer.close if @writer
    Process.waitpid(@worker_pid) if @worker_pid
    @reader.close if @reader
    nil
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
            result = @job.run(data)
            child_writer.write(Oj.dump(result))
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

  def start_input_fiber
    @input_fiber =
      Fiber.new do
        while (data = @input_queue.pop)
          @writer.write(Oj.dump(data))
          Fiber.yield
        end
      end
    @input_fiber.resume
  end

  def start_output_fiber
    @output_fiber =
      Fiber.new do
        Oj.load(@reader) do |data|
          @output_queue.push(data)
          Fiber.yield
        end
      end
    @output_fiber.resume
  end
end
