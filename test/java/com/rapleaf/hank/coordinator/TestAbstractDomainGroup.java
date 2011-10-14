package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainGroup;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;

import java.io.IOException;
import java.util.*;

public class TestAbstractDomainGroup extends BaseTestCase {
  public void testCreateNewFastForwardVersion() throws Exception {
    final Domain d1 = new MockDomain("d1") {
      @Override
      public DomainVersion getLatestVersionNotOpenNotDefunct() throws IOException {
        return new MockDomainVersion(7, 0L);
      }
    };
    final Domain d2 = new MockDomain("d2") {
      @Override
      public DomainVersion getLatestVersionNotOpenNotDefunct() throws IOException {
        return new MockDomainVersion(99, 0L);
      }
    };
    final Domain d3 = new MockDomain("d3");

    DomainGroupVersionDomainVersion d1v1 = new MockDomainGroupVersionDomainVersion(d1, 1);
    DomainGroupVersionDomainVersion d2v1 = new MockDomainGroupVersionDomainVersion(d2, 1);
    final DomainGroupVersion dgv1 = new MockDomainGroupVersion(
        new HashSet<DomainGroupVersionDomainVersion>(Arrays.asList(d1v1, d2v1)), null, 1);

    MockDomainGroup mdg = new MockDomainGroup("dg1") {
      @Override
      public SortedSet<DomainGroupVersion> getVersions() {
        return new TreeSet<DomainGroupVersion>(Arrays.asList(dgv1));
      }

      @Override
      public DomainGroupVersion createNewVersion(Map<Domain, Integer> domainIdToVersion) {
        assertEquals(7, domainIdToVersion.get(d1).intValue());
        assertEquals(99, domainIdToVersion.get(d2).intValue());
        assertNull(domainIdToVersion.get(d3));
        return null;
      }
    };

    mdg.createNewFastForwardVersion();
  }
}
