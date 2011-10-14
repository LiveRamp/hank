package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractRing implements Ring {
  private final int ringNum;
  private final RingGroup ringGroup;

  protected AbstractRing(int ringNum, RingGroup ringGroup) {
    this.ringNum = ringNum;
    this.ringGroup = ringGroup;
  }

  @Override
  public void commandAll(HostCommand command) throws IOException {
    for (Host host : getHosts()) {
      host.enqueueCommand(command);
    }
  }

  @Override
  public Set<Host> getHostsForDomainPartition(Domain domain, int partition) throws IOException {
    Set<Host> results = new HashSet<Host>();
    for (Host host : getHosts()) {
      HostDomain domainById = host.getHostDomain(domain);
      for (HostDomainPartition hdpc : domainById.getPartitions()) {
        if (hdpc.getPartNum() == partition) {
          results.add(host);
          break;
        }
      }
    }
    return results;
  }

  @Override
  public Set<Host> getHostsInState(HostState state) throws IOException {
    Set<Host> results = new HashSet<Host>();
    for (Host host : getHosts()) {
      if (host.getState() == state) {
        results.add(host);
      }
    }
    return results;
  }

  @Override
  public final RingGroup getRingGroup() {
    return ringGroup;
  }

  @Override
  public final int getRingNumber() {
    return ringNum;
  }

  @Override
  public final boolean isUpdatePending() {
    return getUpdatingToVersionNumber() != null;
  }

  @Override
  public Set<Integer> getUnassignedPartitions(Domain domain) throws IOException {
    Set<Integer> unassignedParts = new HashSet<Integer>();
    for (int i = 0; i < domain.getNumParts(); i++) {
      unassignedParts.add(i);
    }

    for (Host hc : getHosts()) {
      HostDomain hdc = hc.getHostDomain(domain);
      if (hdc == null) {
        continue;
      }
      for (HostDomainPartition hdpc : hdc.getPartitions()) {
        unassignedParts.remove(hdpc.getPartNum());
      }
    }

    return unassignedParts;
  }

  @Override
  public boolean isAssigned(DomainGroupVersion domainGroupVersion) throws IOException {
    // Check that each domain of the given domain group version is assigned to this ring
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      // Find all assigned partitions of that domain across hosts
      Set<Integer> assignedPartitions = new HashSet<Integer>();
      for (Host host : getHosts()) {
        HostDomain hostDomain = host.getHostDomain(domain);
        if (hostDomain != null) {
          for (HostDomainPartition partition : hostDomain.getPartitions()) {
            assignedPartitions.add(partition.getPartNum());
          }
        }
      }
      // Check that all of that domain's partitions are assigned at least once. If not, return false.
      if (assignedPartitions.size() != domain.getNumParts()) {
        return false;
      }
    }
    return true;
  }

  public boolean isUpToDate(DomainGroupVersion domainGroupVersion) throws IOException {
    // Check that each domain of the given domain group version is assigned to this ring
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      // Find all assigned partitions of that domain across hosts
      Set<Integer> assignedPartitions = new HashSet<Integer>();
      for (Host host : getHosts()) {
        HostDomain hostDomain = host.getHostDomain(domain);
        if (hostDomain != null) {
          for (HostDomainPartition partition : hostDomain.getPartitions()) {
            // If the partition is not currently at the given domain group version, the ring is not up-to-date
            if (partition.getCurrentDomainGroupVersion() != domainGroupVersion.getVersionNumber()) {
              return false;
            }
            assignedPartitions.add(partition.getPartNum());
          }
        }
      }
      // Check that all of that domain's partitions are assigned at least once. If not, the ring is not up-to-date
      if (assignedPartitions.size() != domain.getNumParts()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareTo(Ring other) {
    return Integer.valueOf(ringNum).compareTo(other.getRingNumber());
  }

  @Override
  public String toString() {
    return String.format("AbstractRing [ringGroup=%s, ring=%d, version=%d, updatingToVersion=%d]",
        (getRingGroup() != null ? getRingGroup().getName() : "null"), this.getRingNumber(), this.getVersionNumber(), this.getUpdatingToVersionNumber());
  }
}
