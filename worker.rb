# frozen_string_literal: true

require "oj"

class Worker
  def initialize(index, input_queue, output_queue, job)
    @index = index
    @input_queue = input_queue
    @output_queue = output_queue
    @job = job

    @threads = []
  end

  def start
    start_fork
    start_input_ractor
    start_output_ractor
  end

  def wait
    @input_ractor.take if @input_ractor
    @writer.close if @writer
    Process.waitpid(@worker_pid) if @worker_pid
    @reader.close if @reader
    @output_ractor.take if @output_ractor
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

  def start_input_ractor
    @input_ractor =
      Ractor.new(@input_queue, @writer) do |input_queue, writer|
        while (data = input_queue.pop)
          writer.write(Oj.dump(data))
          Ractor.receive
        end
      end
  end

  def start_output_ractor
    @output_ractor =
      Ractor.new(
        @output_queue,
        @writer,
        @input_ractor
      ) do |output_queue, reader, input_ractor|
        Oj.load(reader) do |data|
          # signal `input_ractor` to write new data to the fork
          input_ractor.send

          output_queue.push(data)
        end
      end
  end
end
