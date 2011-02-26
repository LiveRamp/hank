namespace java com.rapleaf.hank.generated

union HankResponse {
  /** Equivalent to a "null" value */
  1: bool not_found;
  /** Single-valued result */
  2: binary value;
  /** Multi-valued result for a single key */
  3: list<binary> values;

  /** Multi-key multi-value result */
  //4: map<binary, binary> multivalues;

  // error states

  /** The host queried is not assigned the key that was requested */
  5: bool wrong_host;

  /** The domain_id passed in the request does not correspond to a valid domain */
  6: bool no_such_domain;

  /** There were no available replicas for a given partition */
  7: bool zero_replicas;

  /** There was some internal error in the server. This is pretty bad. */
  8: bool internal_error;
}

service PartDaemon {
  HankResponse get(1:i32 domain_id, 2:binary key);

  //HankResponse multiget(1:byte domain_id, 2:list<binary> keys);
}

service SmartClient {
  HankResponse get(1:string domain_name, 2:binary key);
}