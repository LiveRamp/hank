package com.rapleaf.hank.storage.echo;

import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.ReaderResult;
import com.rapleaf.hank.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;

public class EchoReader implements Reader {
  private final int partNum;

  public EchoReader(int partNum) {
    this.partNum = partNum;
  }

  @Override
  public void get(ByteBuffer key, ReaderResult result) throws IOException {
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

  @Override
  public void close() {
  }
}
