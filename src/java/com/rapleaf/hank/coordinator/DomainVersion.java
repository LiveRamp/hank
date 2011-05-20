package com.rapleaf.hank.coordinator;

public interface DomainVersion extends Comparable<DomainVersion> {
  public int getVersionNumber();

  public long getClosedAt();
}
