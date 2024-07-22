# frozen_string_literal: true

class Consumer
  def initialize(work_queue, writer_queue)
    @input_queue = work_queue
    @writer_queue = writer_queue
  end

  def start
    while (data = @input_queue.pop)
    end
  end
end
