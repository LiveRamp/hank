package com.rapleaf.hank.client;

import com.rapleaf.hank.generated.SmartClient;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;

public interface HankSmartClientIface extends SmartClient.Iface {

  public FutureGet futureGet(String domainName, ByteBuffer key) throws TException;

  public abstract void stop();

}
