package com.liveramp.hank.client;

import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.generated.SmartClient;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;

public interface HankSmartClientIface extends HankClientIface, SmartClient.Iface {

  public HankResponse get(String domain_name, ByteBuffer key);

  public HankBulkResponse getBulk(String domain_name, List<ByteBuffer> keys);

  public FutureGet concurrentGet(String domainName, ByteBuffer key) throws TException;

  public List<FutureGet> concurrentGet(String domainName, List<ByteBuffer> key) throws TException;

  public abstract void stop();

}
