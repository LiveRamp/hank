package com.liveramp.hank.fixtures;

import java.io.FileWriter;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.IntegrationTest;
import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.config.yaml.YamlPartitionServerConfigurator;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.partition_server.PartitionServer;

import static com.liveramp.hank.fixtures.ConfigFixtures.coordinatorConfig;

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
