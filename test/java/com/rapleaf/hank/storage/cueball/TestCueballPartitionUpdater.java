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

package com.rapleaf.hank.storage.cueball;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.storage.IncrementalPartitionUpdater;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;
import org.apache.commons.lang.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestCueballPartitionUpdater extends BaseTestCase {

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
  private CueballPartitionUpdater updater;

  private final String remotePartitionRoot = localTmpDir + "/remote_partition_root";
  private final String localPartitionRoot = localTmpDir + "/partition_root";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    new File(remotePartitionRoot).mkdir();
    new File(localPartitionRoot).mkdir();

    int keyHashSize = 12;
    int valueSize = 5;
    MockCueballMerger merger = new MockCueballMerger();
    CompressionCodec compressionCodec = new NoCompressionCodec();
    int hashIndexBits = 1;

    this.updater = new CueballPartitionUpdater(domain,
        new LocalPartitionRemoteFileOps(remotePartitionRoot, 0),
        keyHashSize,
        valueSize,
        merger,
        compressionCodec,
        hashIndexBits,
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

    // Parent is null when base found
    makeRemoteFile("0/00001.base.cueball");
    assertNull(updater.getParentDomainVersion(v1));
    deleteRemoteFile("0/00001.base.cueball");

    // Parent is previous version number when delta found
    makeRemoteFile("0/00001.delta.cueball");
    assertEquals(v0, updater.getParentDomainVersion(v1));
    deleteRemoteFile("0/00001.delta.cueball");
  }

  public void testDetectCurrentVersionNumber() throws IOException {
    // Null when there is no version
    assertEquals(null, updater.detectCurrentVersionNumber());

    // Nothing when there is only a delta
    makeLocalFile("00001.delta.cueball");
    assertEquals(null, updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.delta.cueball");

    // Correct number when there is a base
    makeLocalFile("00001.base.cueball");
    assertEquals(Integer.valueOf(1), updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.base.cueball");

    // Most recent base
    makeLocalFile("00001.base.cueball");
    makeLocalFile("00002.base.cueball");
    assertEquals(Integer.valueOf(2), updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.base.cueball");
    deleteLocalFile("00002.base.cueball");
  }

  public void testGetCachedVersions() throws IOException {
    Set<DomainVersion> versions = new HashSet<DomainVersion>();

    // Empty cache
    assertEquals(versions, updater.detectCachedBases());
    assertEquals(versions, updater.detectCachedDeltas());

    // Deltas only
    makeLocalCacheFile("00001.delta.cueball");
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>singleton(v1), updater.detectCachedDeltas());
    deleteLocalCacheFile("00001.delta.cueball");

    // Bases only
    makeLocalCacheFile("00000.base.cueball");
    assertEquals(Collections.<DomainVersion>singleton(v0), updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedDeltas());
    deleteLocalCacheFile("00000.base.cueball");

    // Use multiple bases
    makeLocalCacheFile("00000.base.cueball");
    makeLocalCacheFile("00001.base.cueball");
    versions.add(v0);
    versions.add(v1);
    assertEquals(versions, updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedDeltas());
    versions.clear();
    deleteLocalCacheFile("00000.base.cueball");
    deleteLocalCacheFile("00001.base.cueball");
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

    // Fetch delta
    makeRemoteFile("0/00000.delta.cueball");
    updater.fetchVersion(v0, fetchRoot);
    deleteRemoteFile("0/00000.delta.cueball");
    assertTrue(existsLocalFile(fetchRootName + "/00000.delta.cueball"));

    // Fetch base
    makeRemoteFile("0/00000.base.cueball");
    updater.fetchVersion(v0, fetchRoot);
    deleteRemoteFile("0/00000.base.cueball");
    assertTrue(existsLocalFile(fetchRootName + "/00000.base.cueball"));
  }

  public void testUpdate() {
    throw new NotImplementedException();
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
