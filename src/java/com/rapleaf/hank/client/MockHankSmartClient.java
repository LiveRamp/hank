package com.rapleaf.hank.client;

import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockHankSmartClient implements HankSmartClientIface {

  private Map<String, Map<ByteBuffer, HankResponse>> storage = new HashMap<String, Map<ByteBuffer, HankResponse>>();

  public MockHankSmartClient() {
  }

  @Override
  public HankResponse get(String domainName, ByteBuffer key) {
    Map<ByteBuffer, HankResponse> domain;
    domain = storage.get(domainName);
    if (domain == null) {
      return HankResponse.xception(HankException.internal_error("Could not find domain '" + domainName + "'"));
    }
    HankResponse result = domain.get(key);
    if (result == null) {
      return HankResponse.not_found(true);
    } else {
      return result;
    }
  }

  @Override
  public HankBulkResponse getBulk(String domainName, List<ByteBuffer> keys) {
    List<HankResponse> responses = new ArrayList<HankResponse>(keys.size());
    for (ByteBuffer key : keys) {
      responses.add(get(domainName, key));
    }
    return HankBulkResponse.responses(responses);
  }

  @Override
  public FutureGet concurrentGet(String s, ByteBuffer byteBuffer) throws TException {
    throw new NotImplementedException();
  }

  @Override
  public List<FutureGet> concurrentGet(String s, List<ByteBuffer> byteBuffers) throws TException {
    throw new NotImplementedException();
  }

  @Override
  public void stop() {
    // No-op
  }

  public void put(String domain, ByteBuffer key, HankResponse response) {
    if (!storage.containsKey(domain)) {
      storage.put(domain, new HashMap<ByteBuffer, HankResponse>());
    }
    storage.get(domain).put(key, response);
  }

  public void put(String domain, byte[] key, HankResponse response) {
    put(domain, ByteBuffer.wrap(key), response);
  }

  public void put(String domain, ByteBuffer key, ByteBuffer value) {
    put(domain, key, HankResponse.value(value));
  }

  public void put(String domain, ByteBuffer key, byte[] value) {
    put(domain, key, HankResponse.value(value));
  }

  public void put(String domain, byte[] key, ByteBuffer value) {
    put(domain, key, HankResponse.value(value));
  }

  public void put(String domain, byte[] key, byte[] value) {
    put(domain, key, HankResponse.value(value));
  }

  public void clear() {
    storage.clear();
  }
}
