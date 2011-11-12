package com.rapleaf.hank.storage;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestIncrementalPartitionUpdater extends BaseTestCase {

  private IncrementalPartitionUpdater getMockUpdater(final Domain domain,
                                                     final Integer currentVersion) {

    return new IncrementalPartitionUpdater(domain) {

      @Override
      protected Integer detectCurrentVersionNumber() throws IOException {
        return currentVersion;
      }

      @Override
      protected DomainVersion getDomainVersionParent(DomainVersion domainVersion) throws IOException {
        if (domainVersion.getVersionNumber() == 0) {
          return null;
        } else {
          return domain.getVersionByNumber(domainVersion.getVersionNumber() - 1);
        }
      }
    };
  }

  public void testGetVersionsNeededToUpdate() throws IOException {
    final DomainVersion v0 = new MockDomainVersion(0, 1l);
    final DomainVersion v1 = new MockDomainVersion(1, 1l);
    final DomainVersion v2 = new MockDomainVersion(2, 1l);
    final DomainVersion v3 = new MockDomainVersion(3, null);
    final DomainVersion v4 = new MockDomainVersion(4, 1l);

    Domain domain = new MockDomain("domain") {
      @Override
      public DomainVersion getVersionByNumber(int domainVersion) {
        switch (domainVersion) {
          case 0:
            return v0;
          case 1:
            return v1;
          case 2:
            return v2;
          case 3:
            return v3;
          case 4:
            return v4;
          default:
            throw new RuntimeException("Unknown version: " + domainVersion);
        }
      }
    };

    IncrementalPartitionUpdater updater = getMockUpdater(domain, null);

    Set<DomainVersion> versions = new HashSet<DomainVersion>();

    // No need to update to null
    assertEquals(Collections.<DomainVersion>emptySet(),
        updater.getVersionsNeededToUpdate(null, null));

    // Updating from null to v0
    assertEquals(Collections.<DomainVersion>singleton(v0),
        updater.getVersionsNeededToUpdate(null, v0));

    // Updating from null to v1
    versions.add(v0);
    versions.add(v1);
    assertEquals(versions, updater.getVersionsNeededToUpdate(null, v1));
    versions.clear();

    // Updating from v0 to v1
    versions.add(v0);
    versions.add(v1);
    assertEquals(versions,
        updater.getVersionsNeededToUpdate(v0, v1));
    versions.clear();

    // Updating from null to v1, v0 is defunct
    v0.setDefunct(true);
    assertEquals(Collections.<DomainVersion>singleton(v1),
        updater.getVersionsNeededToUpdate(null, v1));
    v0.setDefunct(false);

    // Updating from v1 to v2, v1 is defunct
    v1.setDefunct(true);
    versions.add(v0);
    versions.add(v2);
    assertEquals(versions,
        updater.getVersionsNeededToUpdate(v1, v2));
    v1.setDefunct(false);

    // Updating from null to v4, v3 is not closed
    try {
      updater.getVersionsNeededToUpdate(null, v4);
      fail("Should fail since v3 is not closed");
    } catch (IOException e) {
      // Good
    }
  }
}
