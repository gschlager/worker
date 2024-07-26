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
  TRANSACTION_BATCH_SIZE = 1_000

  def initialize(db_path, queue)
    @db_path = db_path
    @queue = queue
    @statement_counter = 0
  end

  def start
    open_database
    count = 0

    while (data = @queue.pop)
      begin_transaction if @statement_counter.zero?

      @stmt.execute(
        data[:data].fetch_values(:id, :name, :email, :created_at, :bio)
      )

      count += 1
      print "\r#{count}"

      if (@statement_counter += 1) >= TRANSACTION_BATCH_SIZE
        commit_transaction
        @statement_counter = 0
      end
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
    @db.gvl_release_threshold = -1
    @db.execute(SQL_TABLE)

    @stmt = @db.prepare(SQL_INSERT)
  end

  def close
    commit_transaction
    @stmt.close
    @db.close
  end

  def begin_transaction
    return if @db.transaction_active?

    @db.execute("BEGIN DEFERRED TRANSACTION")
  end

  def commit_transaction
    return unless @db.transaction_active?

    @db.execute("COMMIT")
  end
end
