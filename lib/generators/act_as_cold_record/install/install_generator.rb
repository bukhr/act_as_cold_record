# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module ActAsColdRecord
  # Installs PaperTrail in a rails app.
  class InstallGenerator  < ::Rails::Generators::Base
    include ::Rails::Generators::Migration

    source_root File.expand_path("templates", __dir__)

    desc "Generates (but does not run) a migration to add a versions table."

    def create_migration_file
      template = 'create_cold_record_metadata'
      migration_dir = File.expand_path("db/migrate")
      if self.class.migration_exists?(migration_dir, template)
        ::Kernel.warn "Migration already exists: #{template}"
      else
        migration_template(
          "#{template}.rb.erb",
          "db/migrate/#{template}.rb",
          { migration_version: migration_version }
        )
      end
    end

    def self.next_migration_number(dirname)
      ::ActiveRecord::Generators::Base.next_migration_number(dirname)
    end

    def migration_version
      format(
        "[%d.%d]",
        ActiveRecord::VERSION::MAJOR,
        ActiveRecord::VERSION::MINOR
      )
    end
  end
end
