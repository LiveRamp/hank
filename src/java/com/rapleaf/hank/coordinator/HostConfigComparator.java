package com.rapleaf.hank.coordinator;

import java.util.Comparator;

public class HostConfigComparator implements Comparator<HostConfig> {
  @Override
  public int compare(HostConfig arg0, HostConfig arg1) {
    return arg0.getAddress().compareTo(arg1.getAddress());
  }
}
