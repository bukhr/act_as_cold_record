# frozen_string_literal: true

require 'active_support'
require 'active_support/concern'
require "csv"
require "json"

# Allows having records in a cold storage (S3)
module ActAsColdRecord
  module Base
    extend ActiveSupport::Concern

    TIMESTAMP_RENAME = "timestamp_col".freeze
    FORMAT_EQUIVALENCE = {"YYYY-MM" => "%Y-%m", "YYYY" => "%Y", "YYYY-MM-DD" => "%Y-%m-%d"}.freeze
    RETRY_TIMES_FOR_UPLOAD = 3

    module ClassMethods

      # Provides a way to change default variables of act as cold record
      # @param path_keys [Array] The columns to be used as keys (or index).
      # @param timestamp_column [Symbol] The column to be used as default index and filename.
      # @param timestamp_format [String] Granularity of time for store records on S3 files.
      # @return [void]
      # @example Store records by year using key1, key2 and :updated_at as indexes
      #  class MyModel < ApplicationRecord
      #   act_as_cold_record path_keys: [:key1, :key2], timestamp_column: :updated_at, timestamp_format: 'YYYY'
      # end
      def act_as_cold_record(path_keys: [], timestamp_column: :created_at, timestamp_format: "YYYY-MM", compress: true, uuid_for_paths: false)
        @cold_record_path_keys = path_keys
        @cold_record_timestamp_column = timestamp_column
        @cold_record_compress = compress
        raise ArgumentError.new "wrong timestamp format use one of the follow #{FORMAT_EQUIVALENCE.keys}" unless FORMAT_EQUIVALENCE.key?(timestamp_format)

        @cold_record_timestamp_format = timestamp_format
        @cold_record_uuid_for_paths = uuid_for_paths
      end

      # Read records from S3
      # @param start_date [Date] The start date of the records to read.
      # @param end_date [Date] The end date of the records to read.
      # @param path_keys [Hash] Must be subset of those defined in the cold_record_path_keys.
      # @return [Array] The records found.
      # @example
      #  MyModel.read_from_cold_storage(start_date: 2.year.ago, end_date: 1.year.ago, key1: 'value1', key2: 'value2')
      #  f = Tempfile.new
      #  MyModel.read_from_cold_storage(start_date: 2.year.ago, end_date: 1.year.ago, key1: 'value1', key2: 'value2') do |records|
      #    f << records
      #  end
      def read_from_cold_storage(start_date:, end_date:, batch_size: 500, **path_keys)
        validate_args(end_date, path_keys, start_date)

        cold_records = []
        process_archives(start_date, end_date, path_keys, batch_size: batch_size) do |records|
          if block_given?
            yield records
          else
            cold_records.concat(records)
          end
        end
        cold_records
      end

      # Restore records from S3
      # @param start_date [Date] The start date of the records to read.
      # @param end_date [Date] The end date of the records to read.
      # @param path_keys [Hash] Must be subset of those defined in the cold_record_path_keys.
      # @return [Array] The records found.
      # @example
      #  MyModel.restore_from_cold_storage(start_date: 2.year.ago, end_date: 1.year.ago, batch_size: 2000, key1: 'value1', key2: 'value2')
      def restore_from_cold_storage(start_date:, end_date:, batch_size: 500, on_progress: nil, **path_keys)
        validate_args(end_date, path_keys, start_date)
        process_archives(start_date, end_date, path_keys, batch_size: batch_size, on_progress: on_progress, ignore_restored: true) do |records|
          # The stored records are in UTC so we need to mantain that when the import happens
          Time.use_zone("UTC") do
            import!(records, batch_size: batch_size, validate: false, on_duplicate_key_ignore: true)
          end
        end
      end

      # Stores Active Record records in S3 files using the column defined in TIMESTAMP_COLD_RECORD and path_keys as index to build S3 file path.
      # @param start_date [Date, DateTime] The start date of the records to store.
      # @param end_date [Date, DateTime] The end date of the records to store.
      # @param overwrite_policy [Symbol] [:true, :raise, :skip] if archive already exists :true will overwrite the file. :raise, will raise and error and :skip will ignore.
      # @param destroy_after [Boolean] If true, it will destroy the records after storing them.
      #
      # @raise [BackupColdStorageError] If some error happens.
      #
      # @example
      #   MyModel.write_to_cold_storage(start_date: 2.year.ago, end_date: 1.year.ago)
      def write_to_cold_storage(start_date:, end_date:, overwrite_policy: :raise, destroy_after: false)
        validate_dates_args(start_date, end_date)
        cold_record_metadata_by_path = ColdRecordMetadatum.where(model: name).index_by(&:path)
        indexed_archives = {}
        calculate_date_ranges(start_date, end_date) do |initial_date_range, end_date_range|
          # calculate_date_ranges already uses UTC time
          records = where(created_at: initial_date_range..end_date_range)
          attributes_for_keys = generate_attributes_for_keys(records)
          attributes_for_keys.each do |attrs|
            key_path, archive = process_upload(records, cold_record_metadata_by_path, initial_date_range, overwrite_policy, attrs, destroy_after)
            indexed_archives[key_path] = archive if key_path
          end
          destroy_records(records) if destroy_after
        end
      rescue StandardError => error
        rollback_archives(indexed_archives) if indexed_archives.present? && overwrite_policy != :skip
        raise BackupColdStorageError.new(error.message) # rubocop:disable Buk/NotUsageMessageStandardError
      end

      private
      #
      # Destroy the records in batches
      #
      # @param [ActiveRecord::Relation] records
      #
      # @return [nil]
      #
      def destroy_records(records)
        ActiveRecord::Base.transaction do
          records.in_batches do |batch|
            batch.each(&:validate!)
            batch.delete_all #rubocop:disable Buk/PreserveVersioning
          end
        end
      end

      #
      # Process the upload of the records to the cold file.
      # If no records are found on the initial_date, it will return nil and no file will be uploaded
      # If the file already exists and overwrite_policy is false, it will raise an error, otherwise, it will overwrite it
      #
      # @param [ActiveRecord::Relation] records The records in the corresponding dates to upload
      # @param [Hash] cold_record_metadata_by_path The metadata of the records
      # @param [Date] initial_date The initial date of the records
      # @param [Hash] attrs The attributes to further filter the records in the date range
      # @param [Boolean] overwrite_policy If true, it will overwrite the file if it already exists.
      #
      # @return [[string, S3File] || nil] The key path and the archive where the upload was done if it was done
      #                                   or nil if no records were found
      #
      def process_upload(records, cold_record_metadata_by_path, initial_date, overwrite_policy, attrs, destroy_after)
        attrs_for_key = attrs.values
        attrs_for_key << initial_date.strftime(FORMAT_EQUIVALENCE[@cold_record_timestamp_format])
        key = file_key(attrs_for_key)
        uuid_key_path = cold_record_metadata_by_path[key]&.path_uuid
        if uuid_key_path
          case overwrite_policy
          when :raise
            raise ArgumentError, "archive already exists"
          when :skip
            return nil
          end
        end
        records_count = attrs.present? ? records.where(attrs).count : records.count
        return nil if records_count.zero?
        key_path ||= @cold_record_uuid_for_paths ? "#{SecureRandom.uuid}#{archive_extension}" : key
        archive = find_or_create_archive(key_path)
        retries = 0
        begin
          upload_to_archive(archive, records, attrs)
        rescue Aws::S3::MultipartUploadError => e
          if retries < RETRY_TIMES_FOR_UPLOAD
            Rails.logger.info "[ColdRecord] Retrying upload for tenant: #{Tenant.current_tenant.name}, for date: #{initial_date.strftime(FORMAT_EQUIVALENCE[@cold_record_timestamp_format])}"
            retries += 1
            retry
          else
            raise e
          end
        end

        upsert_datum(key, key_path, records_count, destroy_after)
        [key_path, archive]
      end

      #
      # Uploads the records to the cold file.
      #
      # @param [S3File] archive The cold file to upload the records
      # @param [ActiveRecord::Relation] records The records to upload
      # @param [Hash] attrs The attributes to futher filter the records
      #
      # @return [nil]
      #
      def upload_to_archive(archive, records, attrs)
        records = records.where(attrs) if attrs.present?
        sql = <<-SQL
            COPY (
              #{records.to_sql}
            ) TO STDOUT WITH CSV HEADER;
          SQL
        archive.upload_stream do |write_stream|
          write_stream.set_encoding(Encoding::BINARY)
          if @cold_record_compress
            Zlib::GzipWriter.wrap(write_stream) do |gzip_stream|
              ActiveRecord::Base.connection.raw_connection.copy_data(sql) do
                while (row = ActiveRecord::Base.connection.raw_connection.get_copy_data)
                  gzip_stream.write row
                end
              end
            end
          else
            ActiveRecord::Base.connection.raw_connection.copy_data(sql) do
              while (row = ActiveRecord::Base.connection.raw_connection.get_copy_data)
                write_stream.write row
              end
            end
          end
        end
      end

      #
      # Calculates the date ranges to store the records in utc time dependening on the timestamp format.
      #
      # @param [Date] start_date
      # @param [Date] end_date
      #
      # @return [Array] Array of the two dates that represent the range
      #
      def calculate_date_ranges(start_date, end_date)
        case @cold_record_timestamp_format
        when "YYYY-MM-DD"
          start_date.upto(end_date) do |date|
            date = date.in_time_zone("UTC")
            yield [date.beginning_of_day, date.end_of_day]
          end
        when "YYYY-MM"
          current_date = start_date.in_time_zone("UTC")
          while current_date < end_date
            yield [current_date.beginning_of_day, current_date.end_of_month.end_of_day]
            current_date = current_date.next_month.beginning_of_month
          end
        when "YYYY"
          current_date = start_date.in_time_zone("UTC")
          while current_date < end_date
            yield [current_date.beginning_of_day, current_date.end_of_year.end_of_day]
            current_date = current_date.next_year.beginning_of_month
          end
        end
      end

      # Processes an individual archive file, extracting records from it.
      #
      # @param [String] path_uuid The UUID of the path to the archive file.
      # @return [Array<ActiveRecord>] An array of processed records extracted from the archive.
      def process_archive_file(path_uuid, batch_size:)
        inflater = Zlib::Inflate.new(Zlib::MAX_WBITS + 32) if @cold_record_compress
        buffer = ""
        headers = nil
        archive_records = []
        buffer_records = []
        archive = find_archive(path_uuid)
        archive.get do |chunk|
          chunk = inflater.inflate(chunk) if @cold_record_compress

          buffer += chunk
          lines = buffer.lines
          if buffer.end_with?("\n")
            buffer = ""
          else
            buffer = lines.pop
          end

          headers ||= CSV.parse_line(lines.shift)

          record_parsed = []
          CSV.parse(lines.join, headers: headers, encoding: "UTF-8") do |row|
            hashed_row = row.to_h
            columns_enum.each do |column|
              hashed_row[column] = hashed_row[column].to_i if hashed_row[column]
            end
            columns_jsonb.each do |column|
              json_string = hashed_row[column]
              next if json_string.nil?
              json_string = json_string.gsub("=>", ":").gsub("nil", "null")
              hashed_row[column] = JSON.parse(json_string)
            end
            record = new(hashed_row)
            record_parsed << record
          end
          buffer_records.concat(record_parsed)

          if block_given?
            if buffer_records.size >= batch_size
              yield buffer_records.shift(batch_size)
            end
          else
            archive_records.concat(record_parsed)
          end
        end
        # last batch
        yield buffer_records if block_given?
        archive_records
      end

      def process_archives(start_date, end_date, path_keys, batch_size:, on_progress: nil, ignore_restored: false, &block)
        dates = generate_archive_dates(start_date, end_date)
        size = dates.size
        dates.each_with_index do |date, idx|
          metadata_objects = find_cold_record_metadata(date, path_keys)
          metadata_objects.each do |metadatum|
            next if ignore_restored && metadatum.restored == true
            process_archive_file(metadatum.path_uuid, batch_size: batch_size, &block)
          end
          on_progress&.call((idx + 1) * 100 / size)
          metadata_objects.update_all(restored: true, updated_at: Time.zone.now) # rubocop:disable Buk/PreserveVersioning
        end
      end

      # Delete S3 files and cold record tuples.
      # @param uuid_archives [Hash] Uuid and its archive (S3 object).
      # @return [void]
      def rollback_archives(indexed_archives)
        indexed_archives.each_value(&:delete)
        ColdRecordMetadatum.where(path_uuid: indexed_archives.keys).delete_all #rubocop:disable Buk/PreserveVersioning
      end

      # Returns set of combination of all keys.
      # @param cold_records [ActiveRecord::Relation] Records.
      # @return [Array] Relation with combination of keys.
      def generate_attributes_for_keys(cold_records)
        return [{}] if @cold_record_path_keys.empty?
        cold_records.group(*@cold_record_path_keys).select(*@cold_record_path_keys).map do |record|
          record.attributes.except("id", "path_uuid")  # AR select injects id
        end
      end

      # Finds or creates a bucket to have a place to write the records.
      # The last element of the key will be the name of the file.
      # @param [String] key The key to find the archive.
      # @return [Aws::S3::Object] Bucket where the records will be written.
      def find_or_create_archive key
        archive = find_archive key
        begin
          unless archive.exists?
            archive = bucket.put_object({ key: key, body: "" })
          end
          # When the file object does not exists in the bucket, if the bucket is not public, it will raise a Forbidden error.
          # If it exists, it does not raise any error, so we need to rescue the error to create the file.
        rescue Aws::S3::Errors::Forbidden
          archive = bucket.put_object({ key: key, body: "" })
        end
        archive
      end

      # Finds a file in the bucket.
      # @param [String] key The key to find the archive.
      # @return [Aws::S3::Object] The archive found.
      def find_archive key
        bucket.object(key)
      end

      # Returns the bucket where the records are stored.
      # @return [Aws::S3::Bucket]
      def bucket
        @bucket ||= begin
                      s3 = Aws::S3::Resource.new(region: ENV["AWS_REGION"])
                      s3.bucket(Tenant.current_tenant.s3_bucket || ENV["AWS_COLD_RECORDS_BUCKET"])
                    end
      end

      # Returns the path given its components, the last element is the file name with extension.
      # @param [Array] key_names List of elements that make up the path.
      # @return [String] file name
      def file_key(key_names)
        file_name = key_names.last + archive_extension
        ActiveSupport::Cache.expand_cache_key([
                                                Tenant.current_tenant.s3_folder,
                                                table_name + "_storage",
                                                *key_names[0...-1],
                                                file_name,
                                              ])
      end

      def archive_extension
        @cold_record_compress ? ".csv.gz" : ".csv"
      end

      # Retrieve the uuid paths that match the date and path keys.
      # @param [String] date formatted date to filter the records using path.
      # @param [Hash] path_keys The keys to filter the records.
      # @return [Array] The uuid paths from records found.
      def find_cold_record_metadata(date, path_keys)
        filtered_records = ColdRecordMetadatum.where(model: name).where("path LIKE ?", "%#{date}%")
        # Allow search by any subset of keys in any order
        path_keys.each_value do |value|
          filtered_records = filtered_records.where("path LIKE ?", "%#{value}%")
        end
        filtered_records
      end

      # Upserts the metadata of the records.
      # @param [String] file_key full key path.
      # @param [String] path_uuid uuid using as name of S3 file.
      # @param [Integer] records_count The number of records.
      # @param [boolean] destroy_after If the record will be destroyed after storing them.
      # @return [void]
      def upsert_datum(file_key, path_uuid, records_count, destroy_after)
        ColdRecordMetadatum.find_or_initialize_by(model: name, path: file_key, path_uuid: path_uuid) do |datum|
          datum.records_count = records_count
          datum.restored = !destroy_after
          datum.save!
        end
      end

      # Returns the formatted dates between the start_date and end_date as string according to timestamp format
      # @param [Date] start_date
      # @param [Date] end_date
      # @return [Array<String>] The formatted dates.
      def generate_archive_dates(start_date, end_date)
        all_dates_formatted = []
        (start_date..end_date).each do |date|
          all_dates_formatted << date.strftime(FORMAT_EQUIVALENCE[@cold_record_timestamp_format])
        end
        all_dates_formatted.uniq
      end

      # Validates that the path_keys are subset of those defined in the cold_record_path_keys.
      # @param path_keys [Hash] Must be subset of those defined in the cold_record_path_keys.
      # @raise [ArgumentError] If the path_keys are not subset of those defined in the cold_record_path_keys.
      # @return [void]
      def validate_path_keys(path_keys)
        path_keys.each_key do |key|
          unless @cold_record_path_keys.include?(key)
            raise ArgumentError, "Invalid key: #{key}. Valid keys are: #{@cold_record_path_keys}"
          end
        end
      end

      # Validates that the start_date and end_date are of type Date or DateTime
      # @param start_date [Date, DateTime] The start date of the records to store.
      # @param end_date [Date, DateTime] The end date of the records to store.
      # @raise [ArgumentError] If the start_date or end_date are not of type Date or DateTime.
      # @return [void]
      def validate_dates_args(start_date, end_date)
        valid_type = [Date, DateTime]
        if valid_type.exclude?(start_date.class) && valid_type.exclude?(end_date.class)
          raise ArgumentError, "start_date and end_date should be type Date or DateTime"
        end
        raise ArgumentError, "start_date should be before end_date" if start_date > end_date
        raise ArgumentError, "start_date should be beginning of month and end_date should be end of month" if @cold_record_timestamp_format == "YYYY-MM" && (start_date.beginning_of_month != start_date || end_date.end_of_month != end_date)
        raise ArgumentError, "start_date and end_date range should be within a range of years" if @cold_record_timestamp_format == "YYYY" && (start_date.beginning_of_year != start_date || end_date.end_of_year != end_date)
      end

      def validate_args(end_date, path_keys, start_date)
        validate_dates_args(start_date, end_date)
        validate_path_keys(path_keys)
      end

      def columns_jsonb
        @columns_jsonb ||= columns.select { |column| column.sql_type_metadata.sql_type == "jsonb" }.map(&:name)
      end

      def columns_enum
        @columns_enum ||= defined_enums.keys
      end
    end

    # Error for migration errors in the migration process
    class BackupColdStorageError < StandardError
      def initialize(message)
        super("An error occured in the backup process, details: '#{message}'")
      end
    end
  end
end
# frozen_string_literal: true

