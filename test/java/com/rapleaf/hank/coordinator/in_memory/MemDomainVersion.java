package com.rapleaf.hank.coordinator.in_memory;

import com.rapleaf.hank.coordinator.mock.MockDomainVersion;

public class MemDomainVersion extends MockDomainVersion {
  public MemDomainVersion(int versionNumber, Long closedAt) {
    super(versionNumber, closedAt);
  }
}
