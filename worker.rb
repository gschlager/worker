# frozen_string_literal: true

require "msgpack"

class Worker
  def initialize(index, input_queue, output_queue, job)
    @index = index
    @input_queue = input_queue
    @output_queue = output_queue
    @job = job

    @threads = []

    # MessagePack::DefaultFactory.register_type(0x00, Symbol)
  end

  def start
    parent_input_stream, parent_output_stream = IO.pipe
    fork_input_stream, fork_output_stream = IO.pipe

    worker_pid =
      start_fork(
        parent_input_stream,
        parent_output_stream,
        fork_input_stream,
        fork_output_stream
      )

    fork_output_stream.close
    parent_input_stream.close

    start_input_thread(parent_output_stream, worker_pid)
    start_output_thread(fork_input_stream)

    self
  end

  def wait
    @threads.each(&:join)
  end

  private

  def start_fork(
    parent_input_stream,
    parent_output_stream,
    fork_input_stream,
    fork_output_stream
  )
    Process.fork do
      begin
        Process.setproctitle("worker_process#{@index}")

        parent_output_stream.close
        fork_input_stream.close

        stats = { progress: 1, error_count: 0, warning_count: 0 }

        packer = MessagePack::Packer.new(fork_output_stream)

        MessagePack::Unpacker
          .new(parent_input_stream)
          .each do |data|
            result = @job.run(data)
            packer.write({ data: result, stats: })
          end

        packer.flush
      rescue SignalException
        exit(1)
      end
    end
  end

  def start_input_thread(output_stream, worker_pid)
    @threads << Thread.new do
      Thread.current.name = "worker_#{@index}_input"

      begin
        packer = MessagePack::Packer.new(output_stream)

        while (data = @input_queue.pop)
          packer.write(data)
        end

        packer.flush
      ensure
        output_stream.close
        Process.waitpid(worker_pid)
      end
    end
  end

  def start_output_thread(input_stream)
    @threads << Thread.new do
      Thread.current.name = "worker_#{@index}_output"

      begin
        MessagePack::Unpacker
          .new(input_stream)
          .each { |data| @output_queue.push(data) }
      ensure
        input_stream.close
      end
    end
  end
end
