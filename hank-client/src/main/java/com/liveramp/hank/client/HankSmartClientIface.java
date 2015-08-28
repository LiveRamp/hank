package com.liveramp.hank.client;

import java.nio.ByteBuffer;
import java.util.List;

import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.generated.SmartClient;

public interface HankSmartClientIface extends HankClientIface, SmartClient.Iface {

  public HankResponse get(String domain_name, ByteBuffer key);

  public HankBulkResponse getBulk(String domain_name, List<ByteBuffer> keys);

  public FutureGet concurrentGet(String domainName, ByteBuffer key);

  public List<FutureGet> concurrentGet(String domainName, List<ByteBuffer> key);

  public abstract void stop();
}
