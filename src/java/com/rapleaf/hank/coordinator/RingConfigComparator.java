package com.rapleaf.hank.coordinator;

import java.util.Comparator;

public class RingConfigComparator implements Comparator<RingConfig> {
  @Override
  public int compare(RingConfig arg0, RingConfig arg1) {
    return Integer.valueOf(arg0.getRingNumber()).compareTo(arg1.getRingNumber());
  }
}
