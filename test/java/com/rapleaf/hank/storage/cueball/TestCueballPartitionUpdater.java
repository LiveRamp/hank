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
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;

import java.io.File;
import java.io.IOException;

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
  private final CueballPartitionUpdater updater;

  public TestCueballPartitionUpdater() throws IOException {
    this.updater = new CueballPartitionUpdater(domain,
        new LocalPartitionRemoteFileOps(localTmpDir, 1));
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
    makeLocalFile("1/00001.base.cueball");
    assertNull(updater.getParentDomainVersion(v1));
    deleteLocalFile("1/00001.base.cueball");

    // Parent is previous version number when delta found
    makeLocalFile("1/00001.delta.cueball");
    assertEquals(v0, updater.getParentDomainVersion(v1));
    deleteLocalFile("1/00001.delta.cueball");
  }

  private void makeLocalFile(String name) throws IOException {
    File file = new File(localTmpDir + "/" + name);
    file.mkdirs();
    file.createNewFile();
  }

  private void deleteLocalFile(String name) throws IOException {
    new File(localTmpDir + "/" + name).delete();
  }
}
