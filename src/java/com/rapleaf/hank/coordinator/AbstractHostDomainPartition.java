package com.rapleaf.hank.coordinator;


import java.io.IOException;

public abstract class AbstractHostDomainPartition implements HostDomainPartition {
  @Override
  public int compareTo(HostDomainPartition o) {
    return Integer.valueOf(getPartNum()).compareTo(o.getPartNum());
  }

  @Override
  public String toString() {
    try {
      return "AbstractHostDomainPartition [partition number=" + getPartNum() +
          ", current domain group version=" + getCurrentDomainGroupVersion() +
          ", updating to domain group version=" + getUpdatingToDomainGroupVersion() +
          ", deletable=" + isDeletable() + "]";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
