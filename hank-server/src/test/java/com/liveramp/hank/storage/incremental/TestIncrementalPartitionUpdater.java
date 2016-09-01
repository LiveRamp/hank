/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.storage.incremental;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestIncrementalPartitionUpdater extends IncrementalPartitionUpdaterTestCase {

  @Before
  public void setUp() throws Exception {
    new File(remotePartitionRoot).mkdir();
    new File(localPartitionRoot).mkdir();
  }

  @Test
  public void testGetUpdatePlan() throws IOException {
    final DomainVersion v0 = new MockDomainVersion(0, 0l);
    final DomainVersion v1 = new MockDomainVersion(1, 0l);
    final DomainVersion v2 = new MockDomainVersion(2, 0l);
    final DomainVersion v3 = new MockDomainVersion(3, null);
    final DomainVersion v4 = new MockDomainVersion(4, 0l);

    Domain domain = new MockDomain("domain") {
      @Override
      public DomainVersion getVersion(int domainVersion) {
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

    IncrementalUpdatePlanner updatePlanner = new MockIncrementalUpdatePlanner(domain);

    // No need to update to null
    assertNull(updatePlanner.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), null));

    // No need to update from v0 to v0
    assertNull(updatePlanner.computeUpdatePlan(v0, Collections.<DomainVersion>emptySet(), v0));

    // Updating from null to v0
    assertEquals(new IncrementalUpdatePlan(v0, Collections.<DomainVersion>emptyList()),
        updatePlanner.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), v0));

    // Updating from null to v1
    assertEquals(new IncrementalUpdatePlan(v0, v1),
        updatePlanner.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), v1));

    // Updating from v0 to v1
    assertEquals(new IncrementalUpdatePlan(v0, v1),
        updatePlanner.computeUpdatePlan(v0, Collections.<DomainVersion>emptySet(), v1));

    // Updating from null to v1, v0 is defunct
    v0.setDefunct(true);
    try {
      updatePlanner.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), v1);
      fail("Should fail since v1 is not a base");
    } catch (IOException e) {
      // Good
    }
    v0.setDefunct(false);

    // Updating from v1 to v2, v1 is defunct
    v1.setDefunct(true);
    assertEquals(new IncrementalUpdatePlan(v0, v2),
        updatePlanner.computeUpdatePlan(v1, Collections.<DomainVersion>emptySet(), v2));
    v1.setDefunct(false);

    // Updating from null to v4, v3 is not closed
    try {
      updatePlanner.computeUpdatePlan(null, Collections.<DomainVersion>emptySet(), v4);
      fail("Should fail since v3 is not closed");
    } catch (IOException e) {
      // Good
    }

    // Updating from null to v2, v1 is cached
    assertEquals(new IncrementalUpdatePlan(v1, v2),
        updatePlanner.computeUpdatePlan(null, Collections.<DomainVersion>singleton(v1), v2));
  }

  @Test
  public void testGetUpdatePlanNonRelatedVersions() throws IOException {

    final DomainVersion v0 = new MockDomainVersion(0, 0l);
    final DomainVersion v1 = new MockDomainVersion(1, 0l);
    final DomainVersion v2 = new MockDomainVersion(2, 0l);
    final DomainVersion v3 = new MockDomainVersion(3, 0l);

    Domain domain = new MockDomain("domain") {
      @Override
      public DomainVersion getVersion(int domainVersion) {
        switch (domainVersion) {
          case 0:
            return v0;
          case 1:
            return v1;
          case 2:
            return v2;
          case 3:
            return v3;
          default:
            throw new RuntimeException("Unknown version: " + domainVersion);
        }
      }
    };

    IncrementalUpdatePlanner updater =
        new MockIncrementalUpdatePlanner(domain) {
          @Override
          public DomainVersion getParentDomainVersion(DomainVersion version) {
            switch (version.getVersionNumber()) {
              case 0:
                return null;
              case 1:
                return null;
              case 2:
                return v1;
              case 3:
                return v2;
              default:
                throw new RuntimeException("Unknown version: " + version.getVersionNumber());
            }
          }
        };

    // Update to a version non directly related to the current version from v0 to v3, v1 being a base
    assertEquals(new IncrementalUpdatePlan(v1, v2, v3),
        updater.computeUpdatePlan(v0, Collections.<DomainVersion>emptySet(), v3));

    // Update to an older version: from v3 to v2, v1 being a base
    assertEquals(new IncrementalUpdatePlan(v1, v2),
        updater.computeUpdatePlan(v3, Collections.<DomainVersion>emptySet(), v2));
  }

  @Test
  public void testCacheVersionsNeededToUpdate() throws IOException {
    final DomainVersion v0 = new MockDomainVersion(0, 0l);
    final DomainVersion v1 = new MockDomainVersion(1, 0l);

    Domain domain = new MockDomain("domain") {
      @Override
      public DomainVersion getVersion(int domainVersion) {
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
          protected void cleanCachedVersions() throws IOException {
            FileUtils.deleteDirectory(new File(localPartitionRootCache));
          }
        };

    // Update from null with v0
    updater.cacheVersionsNeededToUpdate(null,
        Collections.<DomainVersion>emptySet(),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0));
    assertTrue(existsCacheFile("0.data"));

    // Clean cache
    updater.cleanCachedVersions();
    // Check that cache was cleaned
    assertFalse(new File(updater.localPartitionRootCache).exists());

    // Update from null with v0, v1
    updater.cacheVersionsNeededToUpdate(null,
        Collections.<DomainVersion>emptySet(),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0, v1));
    assertTrue(existsCacheFile("0.data"));
    assertTrue(existsCacheFile("1.data"));

    // Clean cache
    updater.cleanCachedVersions();
    // Check that cache was cleaned
    assertFalse(new File(updater.localPartitionRootCache).exists());

    // Update from v0 with v0, v1
    updater.cacheVersionsNeededToUpdate(v0,
        Collections.<DomainVersion>emptySet(),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0, v1));
    assertFalse(existsCacheFile("0.data"));
    assertTrue(existsCacheFile("1.data"));

    // Clean cache
    updater.cleanCachedVersions();
    // Check that cache was cleaned
    assertFalse(new File(updater.localPartitionRootCache).exists());

    // Update from null with v0, v1 with v0 cached
    updater.cacheVersionsNeededToUpdate(null,
        Collections.<DomainVersion>singleton(v0),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0, v1));
    assertFalse(existsCacheFile("0.data"));
    assertTrue(existsCacheFile("1.data"));

    // Clean cache
    updater.cleanCachedVersions();
    // Check that cache was cleaned
    assertFalse(new File(updater.localPartitionRootCache).exists());
  }

  @Test
  public void testFailingFetcher() throws IOException {
    final DomainVersion v0 = new MockDomainVersion(0, 0l);
    final DomainVersion v1 = new MockDomainVersion(1, 0l);

    Domain domain = new MockDomain("domain") {
      @Override
      public DomainVersion getVersion(int domainVersion) {
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
          protected void cleanCachedVersions() throws IOException {
            FileUtils.deleteDirectory(new File(localPartitionRootCache));
          }
        };

    // Update from null with v0
    updater.cacheVersionsNeededToUpdate(null,
        Collections.<DomainVersion>emptySet(),
        Collections.<DomainVersion>emptySet(),
        new IncrementalUpdatePlan(v0));
    assertTrue(existsCacheFile("0.data"));

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
    assertFalse(existsCacheFile("0.data"));
    assertFalse(existsCacheFile("1.data"));

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

  @Test
  public void testCommitFiles() throws IOException {

    IncrementalPartitionUpdater updater =
        new MockIncrementalPartitionUpdater(localPartitionRoot, null, null);

    String sourceDirRelPath = "source_dir";
    String destinationDirRelPath = "destination_dir";

    File sourceDir = new File(localPartitionRoot + "/" + sourceDirRelPath);
    File destinationDir = new File(localPartitionRoot + "/" + destinationDirRelPath);

    sourceDir.mkdir();
    destinationDir.mkdir();

    assertEquals(0, sourceDir.listFiles().length);
    assertEquals(0, destinationDir.listFiles().length);

    makeLocalFile(sourceDirRelPath + "/a");
    makeLocalFile(sourceDirRelPath + "/b");
    makeLocalFile(sourceDirRelPath + "/c");
    // Sub directory and the files it contains should be ignored
    makeLocalDir(sourceDirRelPath + "/dir");
    makeLocalFile(sourceDirRelPath + "/dir/x");

    assertEquals(4, sourceDir.listFiles().length);
    assertTrue(existsLocalFile(sourceDirRelPath + "/a"));
    assertTrue(existsLocalFile(sourceDirRelPath + "/b"));
    assertTrue(existsLocalFile(sourceDirRelPath + "/c"));
    assertTrue(existsLocalFile(sourceDirRelPath + "/dir"));
    assertTrue(existsLocalFile(sourceDirRelPath + "/dir/x"));

    updater.commitFiles(sourceDir, destinationDir.getAbsolutePath());

    // 1 directory remaining
    assertEquals(1, sourceDir.listFiles().length);
    // 3 files have been moved
    assertEquals(3, destinationDir.listFiles().length);
    assertTrue(existsLocalFile(destinationDirRelPath + "/a"));
    assertTrue(existsLocalFile(destinationDirRelPath + "/b"));
    assertTrue(existsLocalFile(destinationDirRelPath + "/c"));
  }
}
