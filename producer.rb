# frozen_string_literal: true

class Producer
  def initialize(count, queue)
    @count = count
    @queue = queue
  end

  def start
    (1..@count).each do |i|
      @queue << {
        id: i,
        name: "John",
        email: "john@example.com",
        created_at: "2023-12-29T11:10:04Z",
        bio: "a" * 10_000
      }
    end
  end
end
