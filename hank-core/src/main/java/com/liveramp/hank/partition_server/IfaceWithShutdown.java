package com.liveramp.hank.partition_server;

import com.liveramp.hank.generated.PartitionServer.Iface;

public interface IfaceWithShutdown extends Iface {

  public void shutDown() throws InterruptedException;

}
