package com.rapleaf.hank.coordinator;

public interface DomainVersionConfig extends Comparable<DomainVersionConfig> {
  public int getVersionNumber();

  public long getClosedAt();
}
