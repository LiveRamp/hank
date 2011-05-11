package com.rapleaf.hank.ui;

import java.io.IOException;

import org.apache.thrift.TException;

import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.generated.SmartClient;

public interface IClientCache {

  public abstract SmartClient.Iface getSmartClient(RingGroupConfig rgc) throws IOException, TException;

}