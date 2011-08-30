package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;

public class TestAbstractDomainGroup extends BaseTestCase {
  public void testIsDomainRemovable() throws Exception {
    final HashSet<RingGroup> s = new HashSet<RingGroup>();

    Coordinator coord = new MockCoordinator() {
      @Override
      public Set<RingGroup> getRingGroups() {
        return s;
      }
    };

    DomainGroup dg = new AbstractDomainGroup(coord) {
      @Override
      public void setListener(DomainGroupChangeListener listener) {
      }

      @Override
      public boolean removeDomain(Domain domain) throws IOException {
        throw new NotImplementedException();
      }

      @Override
      public SortedSet<DomainGroupVersion> getVersions() throws IOException {
        throw new NotImplementedException();
      }

      @Override
      public DomainGroupVersion getVersionByNumber(int versionNumber) throws IOException {
        throw new NotImplementedException();
      }

      @Override
      public String getName() {
        return "group";
      }

      @Override
      public DomainGroupVersion getLatestVersion() throws IOException {
        throw new NotImplementedException();
      }

      @Override
      public Set<Domain> getDomains() throws IOException {
        throw new NotImplementedException();
      }

      @Override
      public Integer getDomainId(String domainName) throws IOException {
        if (domainName.equals("removable")) {
          return 0;
        }
        if (domainName.equals("unremovable")) {
          return 1;
        }
        throw new IllegalStateException();
      }

      @Override
      public Domain getDomain(int domainId) throws IOException {
        throw new NotImplementedException();
      }

      @Override
      public DomainGroupVersion createNewVersion(Map<Domain, VersionOrAction> domainNameToVersion) throws IOException {
        throw new NotImplementedException();
      }

      @Override
      public void addDomain(Domain domain, int domainId) throws IOException {
        throw new NotImplementedException();
      }
    };

    final MockRingGroup rg1 = new MockRingGroup(dg, "rg1", null);
    s.add(rg1);
    final MockRingGroup rg2 = new MockRingGroup(dg, "rg2", null);
    s.add(rg2);

    
    Domain removableDomain = new MockDomain("removable");
    Domain unremovableDomain = new MockDomain("unremovable");
    assertFalse(dg.isDomainRemovable(unremovableDomain));
    assertTrue(dg.isDomainRemovable(removableDomain));
  }
}
