package com.rapleaf.hank.storage.echo;

import com.rapleaf.hank.test.BaseTestCase;
import com.rapleaf.hank.storage.ReaderResult;

import java.nio.ByteBuffer;

public class TestEchoReader extends BaseTestCase {
  public void testIt() throws Exception {
    EchoReader r = new EchoReader(57);
    ReaderResult result = new ReaderResult();
    r.get(ByteBuffer.wrap(new byte[]{1, 2, 3}), result);
    assertTrue(result.isFound());
    assertEquals("Original value: 01 02 03 Assigned to partition number: 57", new String(result.getBuffer().array()));
  }
}
