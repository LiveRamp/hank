package com.rapleaf.hank.storage.cueball;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class LocalFileOps implements IFileOps {
  private final String remoteRoot;
  private final String localRoot;

  public static class Factory implements IFileOpsFactory {
    @Override
    public IFileOps getFileOps(String localPath, String remotePath) {
      return new LocalFileOps(remotePath, localPath);
    }
  }

  public LocalFileOps(String remoteRoot, String localRoot) {
    this.remoteRoot = remoteRoot;
    this.localRoot = localRoot;
  }

  @Override
  public void copyToLocal(String filePath) throws IOException {
    InputStream in = new FileInputStream(filePath);
    OutputStream out = new FileOutputStream(localRoot + "/" + new File(filePath).getName());
    byte[] buf = new byte[32*1024];
    while (true) {
      int amountRead = in.read(buf);
      if (amountRead == -1) {
        break;
      }
      out.write(buf, 0, amountRead);
    }
    in.close();
    out.close();
  }

  @Override
  public List<String> listFiles() throws IOException {
    File remoteFiles = new File(remoteRoot);
    if (!remoteFiles.exists()) {
      throw new IOException("remote root " + remoteRoot + " does not exist!");
    }

    String[] fileList = remoteFiles.list();
    List<String> fullPaths = new ArrayList<String>(fileList.length);
    for (String fileName : fileList) {
      fullPaths.add(remoteRoot + "/" + fileName);
    }
    return fullPaths;
  }
}
