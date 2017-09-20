package com.liveramp.hank.fixtures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.config.RingGroupConductorConfigurator;
import com.liveramp.hank.ring_group_conductor.RingGroupConductor;

public class RingGroupConductorRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RingGroupConductorRunnable.class);

  private RingGroupConductorConfigurator configurator;
  private RingGroupConductor daemon;

  public RingGroupConductorRunnable(RingGroupConductorConfigurator configurator) throws Exception {
    this.configurator = configurator;
  }

  public void run() {
    try {
      daemon = new RingGroupConductor(configurator);
      daemon.run();
    } catch (Exception e) {
      LOG.error("crap, some exception", e);
    }
  }

  public void pleaseStop() {
    daemon.stop();
  }
}
