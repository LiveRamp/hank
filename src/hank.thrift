namespace java com.rapleaf.hank.generated

union HankExceptions {
  /** The host queried is not assigned the key that was requested */
  1: bool wrong_host;

  /** The domain_id passed in the request does not correspond to a valid domain */
  2: bool no_such_domain;

  /** There were no available replicas for a given partition */
  3: bool zero_replicas;

  /** There was some internal error in the server. This is pretty bad. */
  4: string internal_error;
}

union HankResponse {
  /** Equivalent to a "null" value */
  1: bool not_found;

  /** if found, binary result */
  2: binary value;

  /** error states */
  3: HankExceptions xception;
}

service PartDaemon {
  HankResponse get(1:i32 domain_id, 2:binary key);
}

service SmartClient {
  HankResponse get(1:string domain_name, 2:binary key);
}