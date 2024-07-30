# frozen_string_literal: true

class Job
  def run(data)
    # simulate work
    10.times { |a| 100_000.downto(1) { |b| Math.sqrt(b) * a / 0.2 } }

    data
  end
end
