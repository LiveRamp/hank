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

package com.rapleaf.hank.storage.curly;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.storage.IncrementalPartitionUpdater;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestCurlyPartitionUpdater extends BaseTestCase {

  private final DomainVersion v0 = new MockDomainVersion(0, 0l);
  private final DomainVersion v1 = new MockDomainVersion(1, 0l);
  private final Domain domain = new MockDomain("domain") {
    @Override
    public DomainVersion getVersionByNumber(int versionNumber) {
      switch (versionNumber) {
        case 0:
          return v0;
        case 1:
          return v1;
        default:
          throw new RuntimeException("Unknown version: " + versionNumber);
      }
    }
  };
  private CurlyPartitionUpdater updater;

  private final String remotePartitionRoot = localTmpDir + "/remote_partition_root";
  private final String localPartitionRoot = localTmpDir + "/partition_root";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    new File(remotePartitionRoot).mkdir();
    new File(localPartitionRoot).mkdir();
    this.updater = new CurlyPartitionUpdater(domain,
        new LocalPartitionRemoteFileOps(remotePartitionRoot, 0),
        localPartitionRoot);
  }

  public void testGetDomainVersionParent() throws IOException {

    // Fail when no base or delta was found
    try {
      updater.getParentDomainVersion(v0);
      fail("Should fail");
    } catch (IOException e) {
      // Good
    }

    // Fail when only cueball file was found
    try {
      makeRemoteFile("0/00001.base.cueball");
      updater.getParentDomainVersion(v0);
      fail("Should fail");
    } catch (IOException e) {
      // Good
      deleteRemoteFile("1/00001.base.cueball");
    }

    // Parent is null when base found
    makeRemoteFile("0/00001.base.cueball");
    makeRemoteFile("0/00001.base.curly");
    assertNull(updater.getParentDomainVersion(v1));
    deleteRemoteFile("0/00001.base.cueball");
    deleteRemoteFile("0/00001.base.curly");

    // Parent is previous version number when delta found
    makeRemoteFile("0/00001.delta.cueball");
    makeRemoteFile("0/00001.delta.curly");
    assertEquals(v0, updater.getParentDomainVersion(v1));
    deleteRemoteFile("0/00001.delta.cueball");
    deleteRemoteFile("0/00001.delta.curly");
  }

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

  public void testGetCachedVersions() throws IOException {
    Set<DomainVersion> versions = new HashSet<DomainVersion>();

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

  public void testFetchVersion() throws IOException {
    String fetchRootName = "_fetch";
    String fetchRoot = localPartitionRoot + "/" + fetchRootName;
    new File(fetchRoot).mkdir();

    // Fail when there is no valid file
    try {
      updater.fetchVersion(v0, fetchRoot);
      fail("Should fail");
    } catch (IOException e) {
      // Good
    }

    // Fail when there is only cueball
    try {
      makeRemoteFile("0/00000.delta.cueball");
      updater.fetchVersion(v0, fetchRoot);
      fail("Should fail");
    } catch (IOException e) {
      // Good
      deleteRemoteFile("0/00000.delta.cueball");
    }

    // Fetch delta
    makeRemoteFile("0/00000.delta.cueball");
    makeRemoteFile("0/00000.delta.curly");
    updater.fetchVersion(v0, fetchRoot);
    deleteRemoteFile("0/00000.delta.cueball");
    deleteRemoteFile("0/00000.delta.curly");
    assertTrue(existsLocalFile(fetchRootName + "/00000.delta.cueball"));
    assertTrue(existsLocalFile(fetchRootName + "/00000.delta.curly"));

    // Fetch base
    makeRemoteFile("0/00000.base.cueball");
    makeRemoteFile("0/00000.base.curly");
    updater.fetchVersion(v0, fetchRoot);
    deleteRemoteFile("0/00000.base.cueball");
    deleteRemoteFile("0/00000.base.curly");
    assertTrue(existsLocalFile(fetchRootName + "/00000.base.cueball"));
    assertTrue(existsLocalFile(fetchRootName + "/00000.base.curly"));
  }


  private void makeRemoteFile(String name) throws IOException {
    File file = new File(remotePartitionRoot + "/" + name);
    file.mkdirs();
    file.createNewFile();
  }

  private void deleteRemoteFile(String name) throws IOException {
    new File(remotePartitionRoot + "/" + name).delete();
  }

  private void makeLocalFile(String name) throws IOException {
    File file = new File(localPartitionRoot + "/" + name);
    file.mkdirs();
    file.createNewFile();
  }

  private void deleteLocalFile(String name) throws IOException {
    new File(localPartitionRoot + "/" + name).delete();
  }

  private void makeLocalCacheFile(String name) throws IOException {
    makeLocalFile(IncrementalPartitionUpdater.CACHE_ROOT_NAME + "/" + name);
  }

  private void deleteLocalCacheFile(String name) throws IOException {
    deleteLocalFile(IncrementalPartitionUpdater.CACHE_ROOT_NAME + "/" + name);
  }

  private boolean existsLocalFile(String name) {
    return new File(localPartitionRoot + "/" + name).exists();
  }
}
