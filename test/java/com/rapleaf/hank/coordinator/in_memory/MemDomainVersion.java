package com.rapleaf.hank.coordinator.in_memory;

import com.rapleaf.hank.coordinator.AbstractDomainVersion;

public class MemDomainVersion extends AbstractDomainVersion {
  private final int ver;

  public MemDomainVersion(int ver) {
    this.ver = ver;
  }

  @Override
  public long getClosedAt() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getVersionNumber() {
    return ver;
  }
}
