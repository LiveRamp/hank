namespace java com.liveramp.hank.generated

union HankException {
  /** The host queried is not assigned the key that was requested */
  1: bool wrong_host;

  /** The domain passed in the request does not correspond to a valid domain */
  2: bool no_such_domain;

  /** There was no replica for a given partition */
  3: bool no_replica;

  /** There were no available connections for a given partition */
  4: bool no_connection_available;

  /** Failed to perform query after a specified number of retries */
  5: i32 failed_retries;

  /** There was some internal error in the server. This is pretty bad. */
  6: string internal_error;
}

union HankResponse {
  /* Equivalent to a "null" value */
  1: bool not_found;

  /* If found, binary result */
  2: binary value;

  /* Error states */
  3: HankException xception;
}

union HankBulkResponse {
  /* Individual responses */
  1: list<HankResponse> responses;

  /* Error states */
  2: HankException xception;
}

service PartitionServer {
  HankResponse get(1:i32 domain_id, 2:binary key);
  HankBulkResponse getBulk(1:i32 domain_id, 2:list<binary> keys);
}

service SmartClient {
  HankResponse get(1:string domain_name, 2:binary key);
  HankBulkResponse getBulk(1:string domain_name, 2:list<binary> keys);
}

struct DomainMetadata {
  1: required i32 id;
  2: required i32 num_partitions;
  3: required string storage_engine_factory_class;
  4: required string storage_engine_options;
  5: required string partitioner_class;
  6: required string required_host_flags;
  7: required i32 next_version_number;
}

struct PartitionMetadata {
  1: required i64 num_bytes;
  2: required i64 num_records;
}

struct DomainVersionMetadata {
  1: binary properties;
  2: required map<i32, PartitionMetadata> partitions;
  3: required bool defunct;
  4: required i64 closed_at;
}

struct DomainGroupMetadata {
  1: required map<i32, i32> domain_versions;
}

struct HostDomainPartitionMetadata {
  1: optional i32 current_version_number;
  2: required bool deletable;
}

struct HostDomainMetadata {
  1: required map<i32, HostDomainPartitionMetadata> partitions;
}

struct HostAssignmentsMetadata {
  1: required map<i32, HostDomainMetadata> domains;
}

struct HostMetadata {
  1: required string flags;
  2: required string host_name;
  3: required i32 port_number;
  4: optional map<string, string> environment_flags;
}

struct StatisticsMetadata {
  1: required map<string, string> statistics;
}

struct ClientMetadata {
  1: required string host;
  2: required i64 connected_at;
  3: required string type;
  4: required string version;
}

struct ConnectedServerMetadata {
  1: required string host;
  2: required i32 port_number;
  3: required i64 connected_at;
  4: required map<string, string> environment_flags;
}

struct LatencySampleSummary {

  1: required double minimum;
  2: required double maximum;
  3: required i64 num_values;
  4: required double total;

  6: optional list<double> deciles;
}

struct DomainStatisticsSummary {
  1: required i32 id;
  2: required double throughput_total;
  3: required double response_data_throughput_total;
  4: required i64 num_requests_total;
  5: required i64 num_hits_total;
  6: required i64 num_l1_cache_hits_total;
  7: required i64 num_l2_cache_hits_total;
  8: required i64 cache_num_items;
  9: required i64 cache_max_num_items;
  10: required i64 cache_num_managed_bytes;
  11: required i64 cache_max_num_managed_bytes;

  12: optional LatencySampleSummary latency_summary;

}

struct RuntimeStatisticsSummary {
  1: required map<string, DomainStatisticsSummary> domain_statistics;

}