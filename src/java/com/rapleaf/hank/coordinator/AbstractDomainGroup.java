package com.rapleaf.hank.coordinator;


public abstract class AbstractDomainGroup implements DomainGroup {
  @Override
  public int compareTo(DomainGroup o) {
    return getName().compareTo(o.getName());
  }
}
