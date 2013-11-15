package com.liveramp.hank.storage.echo;

import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.storage.ReaderResult;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestEchoReader extends BaseTestCase {
  @Test
  public void testIt() throws Exception {
    EchoReader r = new EchoReader(57);
    ReaderResult result = new ReaderResult();
    r.get(ByteBuffer.wrap(new byte[]{1, 2, 3}), result);
    assertTrue(result.isFound());
    assertEquals("Original value: 01 02 03 Assigned to partition number: 57", new String(result.getBuffer().array()));
  }
}
