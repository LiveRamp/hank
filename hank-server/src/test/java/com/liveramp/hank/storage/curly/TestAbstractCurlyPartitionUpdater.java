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

package com.liveramp.hank.storage.curly;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.partition_server.PartitionUpdateTaskStatistics;
import com.liveramp.hank.storage.LocalPartitionRemoteFileOps;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.storage.incremental.IncrementalPartitionUpdaterTestCase;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlan;
import org.apache.commons.lang.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAbstractCurlyPartitionUpdater extends IncrementalPartitionUpdaterTestCase {

  private final DomainVersion v0 = new MockDomainVersion(0, 0l, new IncrementalDomainVersionProperties.Base());
  private final DomainVersion v1 = new MockDomainVersion(1, 0l, new IncrementalDomainVersionProperties.Base());
  private final DomainVersion v2 = new MockDomainVersion(2, 0l, new IncrementalDomainVersionProperties.Delta(1));
  private final Domain domain = new MockDomain("domain") {
    @Override
    public DomainVersion getVersion(int versionNumber) {
      switch (versionNumber) {
        case 0:
          return v0;
        case 1:
          return v1;
        case 2:
          return v2;
        default:
          throw new RuntimeException("Unknown version: " + versionNumber);
      }
    }
  };
  private AbstractCurlyPartitionUpdater updater;

  @Before
  public void setUp() throws Exception {

    this.updater = new AbstractCurlyPartitionUpdater(domain,
        new LocalPartitionRemoteFileOps(remotePartitionRoot, 0),
        localPartitionRoot) {
      @Override
      protected boolean shouldFetchCurlyVersion(DomainVersion version) throws IOException {
        return true;
      }

      @Override
      protected void runUpdateCore(DomainVersion currentVersion,
                                   DomainVersion updatingToVersion,
                                   IncrementalUpdatePlan updatePlan,
                                   String updateWorkRoot,
                                   PartitionUpdateTaskStatistics statistics) throws IOException {
        throw new NotImplementedException();
      }
    };

    if (!new File(updateWorkRoot).mkdir()) {
      throw new IOException("Failed to create update work root");
    }
  }

  @Test
  public void testGetDomainVersionParent() throws IOException {
    CurlyUpdatePlanner updatePlanner = new CurlyUpdatePlanner(domain);
    assertNull(updatePlanner.getParentDomainVersion(v0));
    assertNull(updatePlanner.getParentDomainVersion(v1));
    assertEquals(v1, updatePlanner.getParentDomainVersion(v2));
  }

  @Test
  public void testDetectCurrentVersionNumber() throws IOException {
    // Null when there is no version
    assertEquals(null, updater.detectCurrentVersionNumber());

    // Nothing when there is only cueball files
    makeLocalFile("00001.base.cueball");
    assertEquals(null, updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.base.cueball");

    // Nothing when there is only a delta
    makeLocalFile("00001.delta.cueball");
    makeLocalFile("00001.delta.curly");
    assertEquals(null, updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.delta.cueball");
    deleteLocalFile("00001.delta.curly");

    // Correct number when there is a base
    makeLocalFile("00001.base.cueball");
    makeLocalFile("00001.base.curly");
    assertEquals(Integer.valueOf(1), updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.base.cueball");
    deleteLocalFile("00001.base.curly");

    // Most recent base
    makeLocalFile("00001.base.cueball");
    makeLocalFile("00001.base.curly");
    makeLocalFile("00002.base.cueball");
    makeLocalFile("00002.base.curly");
    assertEquals(Integer.valueOf(2), updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.base.cueball");
    deleteLocalFile("00001.base.curly");
    deleteLocalFile("00002.base.cueball");
    deleteLocalFile("00002.base.curly");
  }

  @Test
  public void testGetCachedVersions() throws IOException {
    Set<DomainVersion> versions = new HashSet<DomainVersion>();

    updater.ensureCacheExists();

    // Empty cache
    assertEquals(versions, updater.detectCachedBases());
    assertEquals(versions, updater.detectCachedDeltas());

    // Do not consider cueball files only
    makeLocalCacheFile("00001.base.cueball");
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedDeltas());
    deleteLocalCacheFile("00001.base.cueball");

    // Delta only
    makeLocalCacheFile("00001.delta.cueball");
    makeLocalCacheFile("00001.delta.curly");
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>singleton(v1), updater.detectCachedDeltas());
    deleteLocalCacheFile("00001.delta.cueball");
    deleteLocalCacheFile("00001.delta.curly");

    // Use bases
    makeLocalCacheFile("00000.base.cueball");
    makeLocalCacheFile("00000.base.curly");
    assertEquals(Collections.<DomainVersion>singleton(v0), updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedDeltas());
    deleteLocalCacheFile("00000.base.cueball");
    deleteLocalCacheFile("00000.base.curly");

    // Use multiple bases
    makeLocalCacheFile("00000.base.cueball");
    makeLocalCacheFile("00000.base.curly");
    makeLocalCacheFile("00001.base.cueball");
    makeLocalCacheFile("00001.base.curly");
    versions.add(v0);
    versions.add(v1);
    assertEquals(versions, updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedDeltas());
    versions.clear();
    deleteLocalCacheFile("00000.base.cueball");
    deleteLocalCacheFile("00000.base.curly");
    deleteLocalCacheFile("00001.base.cueball");
    deleteLocalCacheFile("00001.base.curly");
  }

  @Test
  public void testFetchVersion() throws IOException {
    String fetchRootName = "_fetch";
    String fetchRoot = localPartitionRoot + "/" + fetchRootName;
    new File(fetchRoot).mkdir();

    // Fetch delta
    makeRemoteFile("0/00002.delta.cueball");
    makeRemoteFile("0/00002.delta.curly");
    updater.fetchVersion(v2, fetchRoot);
    deleteRemoteFile("0/00002.delta.cueball");
    deleteRemoteFile("0/00002.delta.curly");
    assertTrue(existsLocalFile(fetchRootName + "/00002.delta.cueball"));
    // Curly delta should be fetched
    assertTrue(existsLocalFile(fetchRootName + "/00002.delta.curly"));

    // Fetch base
    makeRemoteFile("0/00000.base.cueball");
    makeRemoteFile("0/00000.base.curly");
    updater.fetchVersion(v0, fetchRoot);
    deleteRemoteFile("0/00000.base.cueball");
    deleteRemoteFile("0/00000.base.curly");
    assertTrue(existsLocalFile(fetchRootName + "/00000.base.cueball"));
    // Curly delta should be fetched
    assertTrue(existsLocalFile(fetchRootName + "/00000.base.curly"));
  }

  @Test
  public void testGetRemotePartitionFilePaths() throws IOException {
    CurlyUpdatePlanner updatePlanner = new CurlyUpdatePlanner(domain);
    List<String> paths = updatePlanner.getRemotePartitionFilePaths(new IncrementalUpdatePlan(v1, v2),
        new LocalPartitionRemoteFileOps(remotePartitionRoot, 0));
    List<String> expectedPaths = new ArrayList<String>();
    expectedPaths.add(getRemoteFilePath("0/00001.base.cueball"));
    expectedPaths.add(getRemoteFilePath("0/00001.base.curly"));
    expectedPaths.add(getRemoteFilePath("0/00002.delta.cueball"));
    expectedPaths.add(getRemoteFilePath("0/00002.delta.curly"));
    Collections.sort(paths);
    Collections.sort(expectedPaths);
    assertEquals(expectedPaths, paths);
  }
}
