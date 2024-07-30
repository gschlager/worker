# frozen_string_literal: true

# frozen_string_literal: true

require "extralite"

class Database
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

  attr_reader :db

  def initialize(db_path, journal_mode = "wal")
    @db_path = db_path
    @journal_mode = journal_mode
    @statement_counter = 0
  end

  def insert(data)
    begin_transaction if @statement_counter.zero?

    @stmt.execute(data.fetch_values(:id, :name, :email, :created_at, :bio))

    if (@statement_counter += 1) >= TRANSACTION_BATCH_SIZE
      commit_transaction
      @statement_counter = 0
    end
  end

  def open_database(init:)
    if init
      db_directory_path = File.dirname(@db_path)
      FileUtils.rm_rf(db_directory_path)
      FileUtils.mkdir_p(db_directory_path)
    end

    @db = Extralite::Database.new(@db_path)
    @db.pragma(
      busy_timeout: 60_000, # 60 seconds
      journal_mode: @journal_mode,
      synchronous: "off",
      temp_store: "memory",
      locking_mode: @journal_mode == "wal" ? "normal" : "exclusive",
      cache_size: -10_000 # 10_000 pages
    )
    @db.execute(SQL_TABLE) if init

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

  def copy_from(source_db_paths)
    table_names = get_table_names
    insert_actions = { "config" => "OR REPLACE", "uploads" => "OR IGNORE" }

    source_db_paths.each do |source_db_path|
      @db.execute("ATTACH DATABASE ? AS source", source_db_path)

      table_names.each do |table_name|
        or_action = insert_actions[table_name] || ""
        @db.execute(
          "INSERT #{or_action} INTO #{table_name} SELECT * FROM source.#{table_name}"
        )
      end

      @db.execute("DETACH DATABASE source")
    end
  end

  def get_table_names
    excluded_table_names = %w[schema_migrations config]
    @db.tables.reject { |t| excluded_table_names.include?(t) }
  end
end
