package com.rapleaf.hank.storage.echo;

import java.nio.ByteBuffer;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.storage.Result;

public class TestEchoReader extends BaseTestCase {
  public void testIt() throws Exception {
    EchoReader r = new EchoReader(57);
    Result result = new Result();
    r.get(ByteBuffer.wrap(new byte[]{1, 2, 3}), result);
    assertTrue(result.isFound());
    assertEquals("Original value: 01 02 03 Assigned to partition number: 57", new String(result.getBuffer().array()));
  }
}
