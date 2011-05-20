package com.rapleaf.hank.coordinator;

import java.util.Comparator;

public class RingGroupConfigComparator implements Comparator<RingGroup> {
  @Override
  public int compare(RingGroup arg0, RingGroup arg1) {
    return arg0.getName().compareTo(arg1.getName());
  }
}
