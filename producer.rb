# frozen_string_literal: true

class Producer
  def initialize(count, queue)
    @count = count
    @queue = queue
  end

  def start
    (1..@count).each do |i|
      @queue << [1, "John", "john@example.com", "2023-12-29T11:10:04Z"]
    end
  end
end
