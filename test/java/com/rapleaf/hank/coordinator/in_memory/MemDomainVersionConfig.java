package com.rapleaf.hank.coordinator.in_memory;

import com.rapleaf.hank.coordinator.AbstractDomainVersionConfig;

public class MemDomainVersionConfig extends AbstractDomainVersionConfig {
  private final int ver;

  public MemDomainVersionConfig(int ver) {
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
