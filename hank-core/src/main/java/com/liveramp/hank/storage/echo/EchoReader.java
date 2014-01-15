package com.liveramp.hank.storage.echo;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.storage.CacheStatistics;
import com.liveramp.hank.storage.Reader;
import com.liveramp.hank.storage.ReaderResult;

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
    sb.append(BytesUtils.bytesToHexString(key));
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
  public CacheStatistics getCacheStatistics() {
    return null;
  }

  @Override
  public void close() {
  }
}
