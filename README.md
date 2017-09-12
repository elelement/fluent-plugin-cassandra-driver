# Cassandra plugin for Fluentd

Cassandra output plugin for Fluentd.

Implemented using the Datastax Ruby Driver for Apache Cassandra gem and targets [CQL3](https://docs.datastax.com/en/cql/3.3/)
and Cassandra 1.2 - 3.x

# Caveats

This project is working and has been tested with the versions:

- Apache Cassandra (3.0.9 y  CQL spec 3.4.0)
- Fluentd (0.14.11):

# Installation

```
td-agent-gem install specific_install
td-agent-gem specific_install https://github.com/elelement/fluent-plugin-cassandra-driver
```

# Quick Start

## Cassandra Configuration
```
# Create Keyspace
CREATE KEYSPACE IF NOT EXISTS DB_USERS WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

# Create Column Family
DROP TABLE IF EXISTS DB_USERS.VISITS;
CREATE TABLE IF NOT EXISTS DB_USERS.VISITS (
    ts timestamp,
    uuid text,
    customer_id int,
    is_recurrent boolean,
    json text,
PRIMARY KEY ((uuid,customer_id),ts)
) WITH CLUSTERING ORDER BY (ts DESC);
```


## Fluentd.conf Configuration

For instance, add HTTP interface to send json payload to Cassandra database. 

```
vi /etc/td-agent/td-agent.conf
```

```
    ## Interface HTTP
    <source>
      @type http
      port 8888
      bind 0.0.0.0
      body_size_limit 32m
      keepalive_timeout 10s
      add_http_headers false 
    </source>
    ## Match STDOUT and Apache Cassandra
    <match cassandra.**>
      type copy
      <store>
        @type stdout
      </store>
      <store>
        type cassandra_driver       # fluent output plugin file name (sans fluent_plugin_ prefix)
        
        hosts 127.0.0.1             # comma delimited string of hosts
        
        keyspace db_users           # cassandra keyspace
        
        column_family visits        # cassandra column family

        ttl 0                       # cassandra ttl (optional, default is 0)
        
        schema '{:uuid => {:uuid => :string}, :customer_id => {:customer_id => :integer}, :is_recurrent => {:is_recurrent => :bool}}'
        
        pop_data_keys false         # pop values from the fluentd hash when storing it as json (optional, default is true)
        
        json_column json            # column where store all remaining data from fluentd (optional)
        
        timestamp_flag true         # flag to enable or disable server 
        timestamp (optional, default is false)
        
        timestamp_column ts         # column where store server timestamp automatically in column family (optional, default is 'ts')
      </store>
    </match>
```

**Schema example**

```
    # hash of hashes :column_damily_key => {:fluentd_record_key => :type_from_list}
    # or :column_damily_key => :type_from_list
    # then :fluentd_record_key will be the same as :column_damily_key
    '{:id => {:ident => nil}, :ts => {:timestamp => :time}}'
```

Available mappings:
* :integer
* :string
* :date
* :timeuuid
* :time
* :bool
    
All nil types will be recognized as string.

# Testing

```
curl -X POST -d 'json={"uuid":"ed21c8d8-95e9-4f16-a3bf-a181e8d0e998","customer_id":2, is_recurrent: false }' "http://localhost:8888/cassandra.visits"
```
