package com.rapleaf.hank.storage.cueball;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HdfsFileOps implements IFileOps {
  private static final Logger LOG = Logger.getLogger(HdfsFileOps.class);

  public static final class Factory implements IFileOpsFactory {
    @Override
    public IFileOps getFileOps(String remoteRoot) {
      try {
        return new HdfsFileOps(remoteRoot);
      } catch (IOException e) {
        throw new RuntimeException("Failed to instantiate "
            + HdfsFileOps.class.getName() + " due to exception!", e);
      }
    }
  }

  private final FileSystem fs;
  private final String remoteRoot;

  public HdfsFileOps(String remoteRoot) throws IOException {
    this.remoteRoot = remoteRoot;
    fs = FileSystem.get(new Configuration());
  }

  @Override
  public String copyToLocal(String remoteFileName, String localDirectory) throws IOException {
    Path remote = new Path(remoteFileName);
    Path local = new Path(localDirectory, remote.getName());
    fs.copyToLocalFile(remote, local);
    return local.toString();
  }

  @Override
  public List<String> listFiles() throws IOException {
    FileStatus[] l = fs.listStatus(new Path(remoteRoot));
    if (l == null) {
      return null;
    }
    List<String> results = new ArrayList<String>(l.length);
    for (FileStatus fileStatus : l) {
      LOG.trace(fileStatus.getPath());
      results.add(fileStatus.getPath().toUri().getPath());
    }
    return results;
  }

  @Override
  public boolean attemptDeleteRemote(String path) throws IOException {
    Path p = new Path(path);
    if (fs.exists(p)) {
      LOG.debug("Deleting " + path);
      fs.delete(p, false);
      return true;
    }
    LOG.debug("Tried to delete " + path + " but it did not exist!");
    return false;
  }
}
