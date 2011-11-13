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

package com.rapleaf.hank.storage;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class TestIncrementalPartitionUpdater extends IncrementalPartitionUpdaterTestCase {

  private final String remotePartitionRoot = localTmpDir + "/remote_partition_root";
  private final String localPartitionRoot = localTmpDir + "/partition_root";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    new File(remotePartitionRoot).mkdir();
    new File(localPartitionRoot).mkdir();
  }

  public void testGetVersionsNeededToUpdate() throws IOException {
    final DomainVersion v0 = new MockDomainVersion(0, 0l);
    final DomainVersion v1 = new MockDomainVersion(1, 0l);
    final DomainVersion v2 = new MockDomainVersion(2, 0l);
    final DomainVersion v3 = new MockDomainVersion(3, null);
    final DomainVersion v4 = new MockDomainVersion(4, 0l);

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

    IncrementalPartitionUpdater updater =
        new MockIncrementalPartitionUpdater(localPartitionRoot, domain, null);

    // No need to update to null
    assertNull(updater.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), null));

    // No need to update from v0 to v0
    assertNull(updater.computeUpdatePlan(v0, Collections.<DomainVersion>emptySet(), v0));

    // Updating from null to v0
    assertEquals(new IncrementalUpdatePlan(v0, Collections.<DomainVersion>emptyList()),
        updater.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), v0));

    // Updating from null to v1
    assertEquals(new IncrementalUpdatePlan(v0, v1),
        updater.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), v1));

    // Updating from v0 to v1
    assertEquals(new IncrementalUpdatePlan(v0, v1),
        updater.computeUpdatePlan(v0, Collections.<DomainVersion>emptySet(), v1));

    // Updating from null to v1, v0 is defunct
    v0.setDefunct(true);
    assertEquals(new IncrementalUpdatePlan(v1),
        updater.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), v1));
    v0.setDefunct(false);

    // Updating from v1 to v2, v1 is defunct
    v1.setDefunct(true);
    assertEquals(new IncrementalUpdatePlan(v0, v2),
        updater.computeUpdatePlan(v1, Collections.<DomainVersion>emptySet(), v2));
    v1.setDefunct(false);

    // Updating from null to v4, v3 is not closed
    try {
      updater.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), v4);
      fail("Should fail since v3 is not closed");
    } catch (IOException e) {
      // Good
    }

    // Updating from null to v2, v1 is cached
    assertEquals(new IncrementalUpdatePlan(v1, v2),
        updater.computeUpdatePlan(null, Collections.<DomainVersion>singleton(v1), v2));
  }

  public void testCacheVersionsNeededToUpdate() throws IOException {
    final DomainVersion v0 = new MockDomainVersion(0, 0l);
    final DomainVersion v1 = new MockDomainVersion(1, 0l);

    Domain domain = new MockDomain("domain") {
      @Override
      public DomainVersion getVersionByNumber(int domainVersion) {
        switch (domainVersion) {
          case 0:
            return v0;
          case 1:
            return v1;
          default:
            throw new RuntimeException("Unknown version: " + domainVersion);
        }
      }
    };
    IncrementalPartitionUpdater updater =
        new MockIncrementalPartitionUpdater(localPartitionRoot, domain, null) {
          @Override
          protected void fetchVersion(DomainVersion version, String fetchRoot) {
            try {
              if (!new File(fetchRoot + "/" + version.getVersionNumber() + ".data").createNewFile()) {
                throw new IOException("Failed to fetch version " + version);
              }
            } catch (IOException e) {
              throw new RuntimeException(e.getMessage(), e);
            }
          }

          @Override
          protected void runUpdateCore(DomainVersion currentVersion,
                                       DomainVersion updatingToVersion,
                                       IncrementalUpdatePlan updatePlan,
                                       String updateWorkRoot) throws IOException {
          }

          @Override
          protected void cleanCachedVersions() throws IOException {
            FileUtils.deleteDirectory(new File(localPartitionRootCache));
          }
        };

    // Update from null with v0
    updater.cacheVersionsNeededToUpdate(null,
        Collections.<DomainVersion>emptySet(),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0));
    assertTrue(cachedFileExists(updater, "0.data"));

    // Clean cache
    updater.cleanCachedVersions();
    // Check that cache was cleaned
    assertFalse(new File(updater.localPartitionRootCache).exists());

    // Update from null with v0, v1
    updater.cacheVersionsNeededToUpdate(null,
        Collections.<DomainVersion>emptySet(),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0, v1));
    assertTrue(cachedFileExists(updater, "0.data"));
    assertTrue(cachedFileExists(updater, "1.data"));

    // Clean cache
    updater.cleanCachedVersions();
    // Check that cache was cleaned
    assertFalse(new File(updater.localPartitionRootCache).exists());

    // Update from v0 with v0, v1
    updater.cacheVersionsNeededToUpdate(v0,
        Collections.<DomainVersion>emptySet(),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0, v1));
    assertFalse(cachedFileExists(updater, "0.data"));
    assertTrue(cachedFileExists(updater, "1.data"));

    // Clean cache
    updater.cleanCachedVersions();
    // Check that cache was cleaned
    assertFalse(new File(updater.localPartitionRootCache).exists());

    // Update from null with v0, v1 with v0 cached
    updater.cacheVersionsNeededToUpdate(null,
        Collections.<DomainVersion>singleton(v0),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0, v1));
    assertFalse(cachedFileExists(updater, "0.data"));
    assertTrue(cachedFileExists(updater, "1.data"));

    // Clean cache
    updater.cleanCachedVersions();
    // Check that cache was cleaned
    assertFalse(new File(updater.localPartitionRootCache).exists());
  }

  public void testFailingFetcher() throws IOException {
    final DomainVersion v0 = new MockDomainVersion(0, 0l);
    final DomainVersion v1 = new MockDomainVersion(1, 0l);

    Domain domain = new MockDomain("domain") {
      @Override
      public DomainVersion getVersionByNumber(int domainVersion) {
        switch (domainVersion) {
          case 0:
            return v0;
          case 1:
            return v1;
          default:
            throw new RuntimeException("Unknown version: " + domainVersion);
        }
      }
    };

    IncrementalPartitionUpdater updater =
        new MockIncrementalPartitionUpdater(localPartitionRoot, domain, null) {
          @Override
          protected void fetchVersion(DomainVersion version, String fetchRoot) {
            // Fail to fetch version 1
            if (version.getVersionNumber() == 1) {
              throw new Error("Failed to fetch version " + version);
            }
            try {
              if (!new File(fetchRoot + "/" + version.getVersionNumber() + ".data").createNewFile()) {
                throw new IOException("Failed to fetch version " + version);
              }
            } catch (IOException e) {
              throw new RuntimeException(e.getMessage(), e);
            }
          }

          @Override
          protected void runUpdateCore(DomainVersion currentVersion,
                                       DomainVersion updatingToVersion,
                                       IncrementalUpdatePlan updatePlan,
                                       String updateWorkRoot) throws IOException {
          }

          @Override
          protected void cleanCachedVersions() throws IOException {
            FileUtils.deleteDirectory(new File(localPartitionRootCache));
          }
        };

    // Update from null with v0
    updater.cacheVersionsNeededToUpdate(null,
        Collections.<DomainVersion>emptySet(),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0));
    assertTrue(cachedFileExists(updater, "0.data"));

    // Clean cache
    updater.cleanCachedVersions();
    // Check that cache was cleaned
    assertFalse(new File(updater.localPartitionRootCache).exists());

    // Update from null with v0, v1 should fail to fetch v1
    try {
      updater.cacheVersionsNeededToUpdate(null,
          Collections.<DomainVersion>emptySet(),
          Collections.<DomainVersion>emptySet(),
          new IncrementalUpdatePlan(v0, v1));
      fail("Should fail");
    } catch (Error e) {
      // Good
    }
    // Check that no file was committed to cache
    assertFalse(cachedFileExists(updater, "0.data"));
    assertFalse(cachedFileExists(updater, "1.data"));

    // Do not clean cache
    // Check that cache was empty
    assertEquals(0, new File(updater.localPartitionRootCache).list().length);

    // Check that there is no remaining fetch root after a fetch failure
    for (File file : new File(updater.localPartitionRoot).listFiles()) {
      if (file.getName().startsWith(IncrementalPartitionUpdater.FETCH_ROOT_PREFIX)) {
        fail("Should not contain any remaining fetch root: " + file.getPath());
      }
    }
  }

  private boolean cachedFileExists(IncrementalPartitionUpdater updater, String fileName) {
    return new File(updater.localPartitionRootCache + "/" + fileName).exists();
  }
}
