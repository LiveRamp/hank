package com.rapleaf.hank.coordinator;

import java.util.Comparator;

public class RingConfigComparator implements Comparator<Ring> {
  @Override
  public int compare(Ring arg0, Ring arg1) {
    return Integer.valueOf(arg0.getRingNumber()).compareTo(arg1.getRingNumber());
  }
}
