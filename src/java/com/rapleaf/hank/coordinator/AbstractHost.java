package com.rapleaf.hank.coordinator;

public abstract class AbstractHost implements Host {
  @Override
  public int compareTo(Host o) {
    return getAddress().compareTo(o.getAddress());
  }
}
