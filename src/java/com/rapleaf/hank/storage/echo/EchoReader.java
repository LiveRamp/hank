package com.rapleaf.hank.storage.echo;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.Result;
import com.rapleaf.hank.util.Bytes;

public class EchoReader implements Reader {
  private final int partNum;

  public EchoReader(int partNum) {
    this.partNum = partNum;
  }

  @Override
  public void get(ByteBuffer key, Result result) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("Original value: ");
    sb.append(Bytes.bytesToHexString(key));
    sb.append(" Assigned to partition number: ");
    sb.append(partNum);

    byte[] bytes = sb.toString().getBytes();
    result.requiresBufferSize(bytes.length);
    System.arraycopy(bytes, 0, result.getBuffer().array(), 0, bytes.length);
    result.found();
  }

  @Override
  public Integer getVersionNumber() {
    return null;
  }
}
