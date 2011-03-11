/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.rapleaf.hank.storage.cueball;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the IFileOps interface for local disk. This File Ops is
 * suitable for use when your datafiles are on local disk or NFS.
 */
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
