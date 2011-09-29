import java.io.File;
import java.nio.ByteBuffer;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.DomainVersionCleaner;
import com.rapleaf.hank.storage.LocalDiskOutputStreamFactory;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.cueball.Cueball;
import com.rapleaf.hank.storage.cueball.LocalFileOps;


public class TestCueballDomainVersionCleaner extends BaseTestCase {
  private String localDiskRoot = localTmpDir + "/local_disk_root";
  private ByteBuffer key = ByteBuffer.wrap(new byte[]{1});
  private ByteBuffer value = ByteBuffer.wrap(new byte[]{2});;

  public void testIt() throws Exception {
    final Cueball storageEngine = new Cueball(1, new Murmur64Hasher(), 1, 1, localDiskRoot, new LocalFileOps.Factory(), NoCompressionCodec.class, "testDomain");
    Writer writer = storageEngine.getWriter(new LocalDiskOutputStreamFactory(localDiskRoot), 0, 1, true);
    writer.write(key, value);
    writer.close();
    writer = storageEngine.getWriter(new LocalDiskOutputStreamFactory(localDiskRoot), 0, 2, false);
    writer.write(key, value);
    writer.close();

    assertTrue(new File(localDiskRoot + "/0/00001.base.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.cueball").exists());

    final DomainVersionCleaner cleaner = storageEngine.getDomainVersionCleaner(null);
    cleaner.cleanVersion(1, 1);

    assertFalse(new File(localDiskRoot + "/0/00001.base.cueball").exists());
    assertTrue(new File(localDiskRoot + "/0/00002.delta.cueball").exists());
  }
}
