/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.rapleaf.hank.storage.incremental;

import com.rapleaf.hank.test.BaseTestCase;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.storage.RemoteDomainVersionDeleter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class TestIncrementalRemoteDomainCleaner extends BaseTestCase {

  private static DomainVersion v0 = new MockDomainVersion(0, (long) 0);
  private static DomainVersion v1 = new MockDomainVersion(1, (long) 0);
  private static DomainVersion v2 = new MockDomainVersion(2, (long) 0);
  private static DomainVersion v3 = new MockDomainVersion(3, (long) 0);
  private static DomainVersion v4 = new MockDomainVersion(4, (long) 0);
  private static DomainVersion v5 = new MockDomainVersion(5, (long) 0);

  private static SortedSet<DomainVersion> versions = new TreeSet<DomainVersion>() {{
    add(v0);
    add(v1);
    add(v2);
    add(v3);
    add(v4);
    add(v5);
  }};

  private static Domain domain = new MockDomain("domain") {
    @Override
    public SortedSet<DomainVersion> getVersions() {
      return versions;
    }
  };

  private static class MockRemoteDomainVersionDeleter implements RemoteDomainVersionDeleter {

    public List<Integer> deletedVersions = new ArrayList<Integer>();

    @Override
    public void deleteVersion(int versionNumber) throws IOException {
      deletedVersions.add(versionNumber);
    }
  }

  private static class MockIncrementalRemoteDomainCleaner extends IncrementalRemoteDomainCleaner {

    public MockIncrementalRemoteDomainCleaner(Domain domain, int numRemoteLeafVersionsToKeep) {
      super(domain, numRemoteLeafVersionsToKeep);
    }

    @Override
    protected DomainVersion getParentDomainVersion(Domain domain, DomainVersion domainVersion) throws IOException {
      switch (domainVersion.getVersionNumber()) {
        case 0:
          return null;
        case 1:
          return v0;
        case 2:
          return v1;
        case 3:
          return v1;
        case 4:
          return null;
        case 5:
          return v4;
        default:
          throw new RuntimeException("Unknown version number");
      }
    }
  }

  public void testZeroVersionsToKeepMeansKeepEverything() throws IOException {
    IncrementalRemoteDomainCleaner cleaner = new MockIncrementalRemoteDomainCleaner(domain, 0);
    MockRemoteDomainVersionDeleter deleter = new MockRemoteDomainVersionDeleter();
    cleaner.deleteOldVersions(deleter);
    assertEquals(0, deleter.deletedVersions.size());
  }

  public void testKeepOneVersion() throws IOException {
    IncrementalRemoteDomainCleaner cleaner = new MockIncrementalRemoteDomainCleaner(domain, 1);
    MockRemoteDomainVersionDeleter deleter = new MockRemoteDomainVersionDeleter();
    cleaner.deleteOldVersions(deleter);
    assertEquals(4, deleter.deletedVersions.size());
    assertTrue(deleter.deletedVersions.contains(0));
    assertTrue(deleter.deletedVersions.contains(1));
    assertTrue(deleter.deletedVersions.contains(2));
    assertTrue(deleter.deletedVersions.contains(3));
  }

  public void testKeepTwoVersions() throws IOException {
    IncrementalRemoteDomainCleaner cleaner = new MockIncrementalRemoteDomainCleaner(domain, 2);
    MockRemoteDomainVersionDeleter deleter = new MockRemoteDomainVersionDeleter();
    cleaner.deleteOldVersions(deleter);
    assertEquals(1, deleter.deletedVersions.size());
    assertTrue(deleter.deletedVersions.contains(2));
  }

  public void testKeepThreeVersions() throws IOException {
    IncrementalRemoteDomainCleaner cleaner = new MockIncrementalRemoteDomainCleaner(domain, 3);
    MockRemoteDomainVersionDeleter deleter = new MockRemoteDomainVersionDeleter();
    cleaner.deleteOldVersions(deleter);
    assertEquals(0, deleter.deletedVersions.size());
  }
}
