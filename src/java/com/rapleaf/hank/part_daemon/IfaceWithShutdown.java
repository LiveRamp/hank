package com.rapleaf.hank.part_daemon;

import com.rapleaf.hank.generated.PartDaemon.Iface;

public interface IfaceWithShutdown extends Iface {

  public void shutDown() throws InterruptedException;

}