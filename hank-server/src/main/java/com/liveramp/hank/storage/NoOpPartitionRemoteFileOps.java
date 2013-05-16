package com.liveramp.hank.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class NoOpPartitionRemoteFileOps implements PartitionRemoteFileOps {

  public static class Factory implements PartitionRemoteFileOpsFactory {
    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteRoot, int partitionNumber) throws IOException {
      return new NoOpPartitionRemoteFileOps();
    }
  }

  @Override
  public InputStream getInputStream(String remoteRelativePath) throws IOException {
    return new InputStream() {
      @Override
      public int read() throws IOException {
        // No-op
        return 0;
      }
    };
  }

  @Override
  public OutputStream getOutputStream(String remoteRelativePath) throws IOException {
    return new OutputStream() {

      @Override
      public void write(int i) throws IOException {
        // No-op
      }
    };
  }

  @Override
  public boolean exists(String remoteRelativePath) throws IOException {
    return false;
  }

  @Override
  public void copyToLocalRoot(String remoteSourceRelativePath, String localDestinationRoot) throws IOException {
    // No-op
  }

  @Override
  public boolean attemptDelete(String remoteRelativePath) throws IOException {
    return false;
  }

  @Override
  public String getRemoteAbsolutePath(String remoteRelativePath) {
    return null;
  }
}
