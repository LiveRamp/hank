package com.rapleaf.hank.storage.cueball;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileOps implements IFileOps {
  public static final class Factory implements IFileOpsFactory {
    @Override
    public IFileOps getFileOps(String localPath, String remotePath) {
      try {
        return new HdfsFileOps(localPath, remotePath);
      } catch (IOException e) {
        throw new RuntimeException("Failed to instantiate "
            + HdfsFileOps.class.getName() + " due to exception!", e);
      }
    }
  }

  private final FileSystem fs;
  private final String localRoot;
  private final String remoteRoot;

  public HdfsFileOps(String localRoot, String remoteRoot) throws IOException {
    this.localRoot = localRoot;
    this.remoteRoot = remoteRoot;
    fs = FileSystem.get(new Configuration());
  }

  @Override
  public void copyToLocal(String fileName) throws IOException {
    Path remote = new Path(fileName);
    fs.copyToLocalFile(remote, new Path(localRoot, remote.getName()));
  }

  @Override
  public List<String> listFiles() throws IOException {
    FileStatus[] l = fs.listStatus(new Path(remoteRoot));
    List<String> results = new ArrayList<String>(l.length);
    for (FileStatus fileStatus : l) {
      results.add(fileStatus.getPath().toUri().getPath());
    }
    return results;
  }
}
