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
import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.LocalPartitionFileStreamFactory;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;
import com.rapleaf.hank.storage.RemoteDomainVersionDeleter;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;

import java.io.File;
import java.nio.ByteBuffer;

public class TestCueballRemoteDomainVersionDeleter extends BaseTestCase {

  private String localDiskRoot = localTmpDir + "/local_disk_root";
  private ByteBuffer key = ByteBuffer.wrap(new byte[]{1});
  private ByteBuffer value = ByteBuffer.wrap(new byte[]{2});

  public void testIt() throws Exception {
    final Cueball storageEngine = new Cueball(1,
        new Murmur64Hasher(), 1, 1, localDiskRoot,
        new LocalPartitionRemoteFileOps.Factory(), NoCompressionCodec.class,
        new MockDomain("domain", 0, 1, null, null, null, null), 0, -1);
    Writer writer = storageEngine.getWriter(new MockDomainVersion(1, 0L, new IncrementalDomainVersionProperties.Base()),
        new LocalPartitionFileStreamFactory(localDiskRoot), 0);
    writer.write(key, value);
    writer.close();
    writer = storageEngine.getWriter(new MockDomainVersion(2, 0L, new IncrementalDomainVersionProperties.Delta(1)),
        new LocalPartitionFileStreamFactory(localDiskRoot), 0);
    writer.write(key, value);
    writer.close();

    assertTrue(new File(localDiskRoot + "/0/00001.base.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.cueball").exists());

    final RemoteDomainVersionDeleter cleaner = storageEngine.getRemoteDomainVersionDeleter();
    cleaner.deleteVersion(1);

    assertFalse(new File(localDiskRoot + "/0/00001.base.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.cueball").exists());
  }
}
