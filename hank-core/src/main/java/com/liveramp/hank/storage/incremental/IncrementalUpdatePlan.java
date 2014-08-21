package com.liveramp.hank.storage.incremental;

import com.liveramp.hank.coordinator.DomainVersion;

import java.util.*;

public class IncrementalUpdatePlan {

  private final DomainVersion base;
  private final List<DomainVersion> deltasOrdered;

  public IncrementalUpdatePlan(DomainVersion base) {
    this(base, Collections.<DomainVersion>emptyList());
  }

  public IncrementalUpdatePlan(DomainVersion base, DomainVersion delta) {
    this.base = base;
    this.deltasOrdered = new ArrayList<DomainVersion>(1);
    this.deltasOrdered.add(delta);
  }

  public IncrementalUpdatePlan(DomainVersion base, List<DomainVersion> deltasOrdered) {
    this.base = base;
    this.deltasOrdered = deltasOrdered;
  }

  public IncrementalUpdatePlan(DomainVersion base, DomainVersion... deltas) {
    this(base, Arrays.asList(deltas));
  }

  public DomainVersion getBase() {
    return base;
  }

  public List<DomainVersion> getDeltasOrdered() {
    return deltasOrdered;
  }

  public Set<DomainVersion> getAllVersions() {
    Set<DomainVersion> result = new HashSet<DomainVersion>();
    result.add(base);
    result.addAll(deltasOrdered);
    return result;
  }

  @Override
  public String toString() {
    return "IncrementalUpdatePlan [base=" + base
        + ", deltas=" + deltasOrdered + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof IncrementalUpdatePlan) {
      IncrementalUpdatePlan plan = (IncrementalUpdatePlan) other;
      return this.base.equals(plan.base) && this.deltasOrdered.equals(plan.deltasOrdered);
    } else {
      return false;
    }
  }
}
