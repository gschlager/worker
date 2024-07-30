# frozen_string_literal: true

require_relative "database"

class Job
  def run(data)
    # simulate work
    10.times { |a| 100_000.downto(1) { |b| Math.sqrt(b) * a / 0.2 } }

    @db.insert(data)
  end

  def open_db(db_path)
    @db = Database.new(db_path)
    @db.open_database(init: true)
  end

  def close_db
    @db.close
  end
end
