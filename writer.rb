# frozen_string_literal: true

require "extralite"

class Writer
  SQL_TABLE = <<~SQL
    CREATE TABLE users (
      id          INTEGER,
      name        TEXT,
      email       TEXT,
      created_at  DATETIME,
      bio         TEXT
    )
  SQL
  SQL_INSERT = "INSERT INTO users VALUES (?, ?, ?, ?, ?)"

  def initialize(db_path, queue)
    @db_path = db_path
    @queue = queue
  end

  def start
    open_database

    while (data = @queue.pop)
      @stmt.execute(data.fetch_values(:id, :name, :email, :created_at, :bio))
    end

    close
  end

  private

  def open_database
    db_directory_path = File.dirname(@db_path)
    FileUtils.rm_rf(db_directory_path)
    FileUtils.mkdir_p(db_directory_path)

    @db = Extralite::Database.new(@db_path)
    @db.pragma(
      busy_timeout: 60_000, # 60 seconds
      journal_mode: "wal",
      synchronous: "off"
    )
    @db.execute(SQL_TABLE)

    @stmt = @db.prepare(SQL_INSERT)
  end

  def close
    @stmt.close
    @db.close
  end
end
