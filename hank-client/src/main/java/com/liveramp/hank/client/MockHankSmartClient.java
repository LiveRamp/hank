package com.liveramp.hank.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankException;
import com.liveramp.hank.generated.HankResponse;

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

  private class GetTaskRunnable implements GetTaskRunnableIface {

    private final String domain;
    private final ByteBuffer key;
    private HankResponse response = null;

    private GetTaskRunnable(String domain, ByteBuffer key) {
      this.domain = domain;
      this.key = key;
    }

    @Override
    public void run() {
      response = get(domain, key);
    }

    @Override
    public HankResponse getResponse() {
      return response;
    }
  }

  @Override
  public FutureGet concurrentGet(String domainName, ByteBuffer key) {
    FutureGet futureGet = new FutureGet(new GetTaskRunnable(domainName, key));
    futureGet.run();
    return futureGet;
  }

  @Override
  public List<FutureGet> concurrentGet(String domainName, List<ByteBuffer> keys) {
    List<FutureGet> result = new ArrayList<FutureGet>();
    for (ByteBuffer key : keys) {
      FutureGet futureGet = new FutureGet(new GetTaskRunnable(domainName, key));
      futureGet.run();
      result.add(futureGet);
    }
    return result;
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

  public void empty(String domain) {
    storage.put(domain, new HashMap<ByteBuffer, HankResponse>());
  }

  public void clear() {
    storage.clear();
  }
}
