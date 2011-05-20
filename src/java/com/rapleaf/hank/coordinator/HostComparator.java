package com.rapleaf.hank.coordinator;

import java.util.Comparator;

public class HostComparator implements Comparator<Host> {
  @Override
  public int compare(Host arg0, Host arg1) {
    return arg0.getAddress().compareTo(arg1.getAddress());
  }
}
