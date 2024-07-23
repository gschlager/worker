# frozen_string_literal: true

require_relative "database"

class Job
  def run(data)
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
