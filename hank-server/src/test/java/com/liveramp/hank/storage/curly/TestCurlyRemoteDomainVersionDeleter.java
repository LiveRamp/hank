/**
 * Copyright 2013 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.hank.storage.curly;

import java.io.File;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.liveramp.hank.compression.cueball.NoCueballCompressionCodec;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.hasher.Murmur64Hasher;
import com.liveramp.hank.storage.LocalPartitionRemoteFileOps;
import com.liveramp.hank.storage.RemoteDomainVersionDeleter;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.Writer;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.test.ZkTestCase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCurlyRemoteDomainVersionDeleter extends ZkTestCase {
  private String localDiskRoot = localTmpDir + "/local_disk_root";
  private ByteBuffer key = ByteBuffer.wrap(new byte[]{1});
  private ByteBuffer value = ByteBuffer.wrap(new byte[]{2});

  @Test
  public void testIt() throws Exception {
    final Curly storageEngine = new Curly(1, new Murmur64Hasher(), 100000, 1, 1000, localDiskRoot,
        localDiskRoot, new LocalPartitionRemoteFileOps.Factory(), NoCueballCompressionCodec.class,
        new MockDomain("domain", 0, 1, null, null, null, null),
        0, -1, null, -1, -1);
    Writer writer = storageEngine.getWriter(new MockDomainVersion(1, 0L, new IncrementalDomainVersionProperties.Base()),
        new LocalPartitionRemoteFileOps(localDiskRoot, 0), 0);
    writer.write(key, value);
    writer.close();
    writer = storageEngine.getWriter(new MockDomainVersion(2, 0L, new IncrementalDomainVersionProperties.Delta(1)),
        new LocalPartitionRemoteFileOps(localDiskRoot, 0), 0);
    writer.write(key, value);
    writer.close();

    assertTrue(new File(localDiskRoot + "/0/00001.base.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00001.base.curly").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.curly").exists());

    final RemoteDomainVersionDeleter cleaner = storageEngine.getRemoteDomainVersionDeleter(StorageEngine.RemoteLocation.DOMAIN_BUILDER);
    cleaner.deleteVersion(1);

    assertFalse(new File(localDiskRoot + "/0/00001.base.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.cueball").exists());
    assertFalse(new File(localDiskRoot + "/0/00001.base.curly").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.curly").exists());
  }
}
