package com.liveramp.hank.ui;

import java.io.IOException;

import org.apache.thrift.TException;

import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.generated.SmartClient;

public interface IClientCache {

  public abstract SmartClient.Iface getSmartClient(RingGroup rgc) throws IOException, TException;

}
