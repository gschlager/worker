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

  def initialize(db_path)
    @db_path = db_path
  end

  def insert(data)
    @stmt.execute(data.fetch_values(:id, :name, :email, :created_at, :bio))
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
      journal_mode: "wal",
      synchronous: "off"
    )
    @db.execute(SQL_TABLE) if init

    @stmt = @db.prepare(SQL_INSERT)
  end

  def close
    @stmt.close
    @db.close
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
