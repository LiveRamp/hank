namespace java com.rapleaf.tiamat.generated

union TiamatResponse {
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

  /** There was some internal error in the server. This is pretty bad. */
  7: bool internal_error;
}

service Tiamat {
  TiamatResponse get(1:byte domain_id, 2:binary key);

  //TiamatResponse multiget(1:byte domain_id, 2:list<binary> keys);
}