package com.rapleaf.hank.storage.curly;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.DomainVersionCleaner;
import com.rapleaf.hank.storage.LocalDiskOutputStreamFactory;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;
import com.rapleaf.hank.storage.Writer;

import java.io.File;
import java.nio.ByteBuffer;

public class TestCurlyDomainVersionCleaner extends ZkTestCase {
  private String localDiskRoot = localTmpDir + "/local_disk_root";
  private ByteBuffer key = ByteBuffer.wrap(new byte[]{1});
  private ByteBuffer value = ByteBuffer.wrap(new byte[]{2});

  public void testIt() throws Exception {
    final Curly storageEngine = new Curly(1, new Murmur64Hasher(), 100000, 1, 1000, localDiskRoot,
      new LocalPartitionRemoteFileOps.Factory(), NoCompressionCodec.class,
      new MockDomain("domain", 0, 1, null, null, null, null));
    Writer writer = storageEngine.getWriter(new LocalDiskOutputStreamFactory(localDiskRoot), 0, 1, true);
    writer.write(key, value);
    writer.close();
    writer = storageEngine.getWriter(new LocalDiskOutputStreamFactory(localDiskRoot), 0, 2, false);
    writer.write(key, value);
    writer.close();

    assertTrue(new File(localDiskRoot + "/0/00001.base.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00001.base.curly").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.curly").exists());

    final DomainVersionCleaner cleaner = storageEngine.getDomainVersionCleaner(null);
    cleaner.cleanVersion(1);

    assertFalse(new File(localDiskRoot + "/0/00001.base.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.cueball").exists());
    assertFalse(new File(localDiskRoot + "/0/00001.base.curly").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.curly").exists());
  }
}
