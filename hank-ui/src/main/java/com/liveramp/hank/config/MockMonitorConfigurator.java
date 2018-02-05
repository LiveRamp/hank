package com.liveramp.hank.config;

import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.monitor.notifier.Notifier;

public class MockMonitorConfigurator implements MonitorConfigurator{
  @Override
  public List<Notifier> getGlobalNotifiers() throws InvalidConfigurationException {
    return Lists.newArrayList();
  }

  @Override
  public List<Notifier> getRingGroupNotifiers(RingGroup ringGroup) throws InvalidConfigurationException {
    return Lists.newArrayList();
  }
}
