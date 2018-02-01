package com.liveramp.hank.fixtures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.partition_server.PartitionServer;

public class PartitionServerRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionServerRunnable.class);

  private PartitionServer server;
  private final PartitionServerConfigurator configurator;

  public PartitionServerRunnable(PartitionServerConfigurator configurator) throws Exception {
    this.configurator = configurator;
  }

  public void run() {
    try {
      server = new PartitionServer(configurator, "localhost");
      server.run();
    } catch (Throwable t) {
      LOG.error("crap, some exception...", t);
    }
  }

  public void pleaseStop() throws Exception {
    server.stopSynchronized();
  }
}
