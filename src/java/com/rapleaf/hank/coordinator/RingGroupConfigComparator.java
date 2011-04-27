package com.rapleaf.hank.coordinator;

import java.util.Comparator;

public class RingGroupConfigComparator implements Comparator<RingGroupConfig> {
  @Override
  public int compare(RingGroupConfig arg0, RingGroupConfig arg1) {
    return arg0.getName().compareTo(arg1.getName());
  }
}
