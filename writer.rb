# frozen_string_literal: true

require "extralite"

class Writer
  SQL_TABLE = <<~SQL
    CREATE TABLE users (
      id          INTEGER,
      name        TEXT,
      email       TEXT,
      created_at  DATETIME
    )
  SQL
  SQL_INSERT = "INSERT INTO users VALUES (?, ?, ?, ?)"

  def initialize(db_path, queue)
    @db_path = db_path
    @queue = queue
    open_database
  end

  def start
    while (data = @queue.pop)
      @stmt.execute(data)
    end

    @stmt.close
    @db.close
  end

  private

  def open_database
    FileUtils.mkdir_p(File.dirname(@db_path))

    @db = Extralite::Database.new(@db_path)
    @db.pragma(
      busy_timeout: 60_000, # 60 seconds
      journal_mode: "wal",
      synchronous: "off"
    )
    @db.execute(SQL_TABLE)

    @stmt = @db.prepare(SQL_INSERT)
  end
end
