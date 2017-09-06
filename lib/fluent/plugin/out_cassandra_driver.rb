require 'cassandra'
require 'msgpack'
require 'json'
require 'date'

module Fluent
  class CassandraCqlOutput < BufferedOutput
    Fluent::Plugin.register_output('cassandra_driver', self)

    config_param :hosts, :string
    config_param :keyspace, :string
    config_param :column_family, :string
    config_param :ttl, :integer, :default => 0
    config_param :schema, :string

    # remove keys from the fluentd json event as they're processed
    # for individual columns?
    config_param :pop_data_keys, :bool, :default => true

    # column to store all data keys as json
    config_param :json_column, :string, :default => false

    # column to store ti
    config_param :timestamp_flag, :bool, :default => false
    config_param :timestamp_column, :string, :default => "ts"

    def session
      @session ||= get_session(self.hosts, self.keyspace)
    end

    def configure(conf)
      super

      # perform validations
      raise ConfigError, "'Hosts' is required by Cassandra output (ex: localhost, 127.0.0.1, ec2-54-242-141-252.compute-1.amazonaws.com" if self.hosts.nil?
      raise ConfigError, "'Keyspace' is required by Cassandra output (ex: FluentdLoggers)" if self.keyspace.nil?
      raise ConfigError, "'ColumnFamily' is required by Cassandra output (ex: events)" if self.column_family.nil?
      raise ConfigError, "'Schema' is required by Cassandra output" if self.schema.nil?

      # convert schema from string to hash
      # NOTE: ok to use eval b/c this isn't this isn't a user
      #       supplied string
      self.schema = eval(self.schema)

      raise ConfigError, "'Schema' must contain at least one column" if self.schema.keys.length < 1

      # split hosts to array
      self.hosts = self.hosts.split(',')
    end

    def start
      super
      session
    end

    def shutdown
      super
      @session.close if @session
    end

    def format(tag, time, record)
      record.to_msgpack
    end

    def write(chunk)
      chunk.msgpack_each { |record|
        
        $log.debug "New Record to Cassandra: #{record.to_json}"
        values = build_insert_values(record)
        # Query
        query = "INSERT INTO #{self.column_family} (#{values.keys.join(',')}" + ( self.timestamp_flag ? ", #{self.timestamp_column}" : "" ) + ") " \
                "VALUES (#{values.keys.map { |key| ":#{key}" }.join(',')}" + ( self.timestamp_flag ? ", toUnixTimestamp(now())" : "" ) + ") " \
                "USING TTL #{self.ttl}"
        # Prepare Query
        insert  = @session.prepare(query)

        # Start Transaction
        begin
          @session.execute(insert, arguments: values)
        rescue Exception => e
          $log.error "Cannot send record to Cassandra: #{e.message}\nTrace: #{e.backtrace.to_s}"
          raise e
        end
      }
    end

    private

    def is_boolean(op)
      return op && true
    end

    def get_session(hosts, keyspace)
      cluster = ::Cassandra.cluster(hosts: hosts)

      cluster.connect(keyspace)
    end

    def build_insert_values(record)
      values = self.schema.map { |column_family_key, mapping|
        if mapping.class == Hash
          record_key, type = mapping.first
        else
          record_key, type = column_family_key, mapping
        end

        value = record[record_key.to_s]

        case type
          when :integer
            value = value.to_i
          when :float
            value = value.to_f
          when :date
-            value = Date.parse(value)
          when :timeuuid
            value = ::Cassandra::Uuid::Generator.new.at(Time.parse(value))
          when :time
            value = Time.parse(value)
          when :bool
            if is_boolean(value)
              value = true
            else
              value = false
            end
          when :string
          else
            value = value.to_s
        end

        [column_family_key.to_s.downcase, value]
      }.to_h

      self.schema.each { |column_family_key, mapping|
        record_key = mapping.class == Hash ? mapping.first.first : column_family_key

        record.delete(record_key.to_s)
      } if self.pop_data_keys

      # if we have one more data in record and json column
      # then store all remaining data into that column
      values[self.json_column] = record.to_json if self.json_column and record.length > 0

      values
    end
  end
end
