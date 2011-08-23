package com.rapleaf.hank.partition_server;

import com.rapleaf.hank.generated.PartitionServer.Iface;

public interface IfaceWithShutdown extends Iface {

  public void shutDown() throws InterruptedException;

}
