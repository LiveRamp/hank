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

package com.rapleaf.hank.storage;

import org.apache.commons.io.FileUtils;

import java.io.*;

public class LocalPartitionRemoteFileOps implements PartitionRemoteFileOps {

  public static class Factory implements PartitionRemoteFileOpsFactory {

    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteRoot, int partitionNumber) throws IOException {
      return new LocalPartitionRemoteFileOps(remoteRoot, partitionNumber);
    }
  }

  private final String partitionRoot;

  public LocalPartitionRemoteFileOps(String remoteDomainRoot,
                                     int partitionNumber) throws IOException {
    this.partitionRoot = remoteDomainRoot + "/" + partitionNumber;
    if (!new File(partitionRoot).isAbsolute()) {
      throw new IOException("Cannot initialize " + this.getClass().getSimpleName()
          + " with a non absolute remote partition root: "
          + partitionRoot);
    }
  }

  @Override
  public InputStream getInputStream(String remoteRelativePath) throws IOException {
    String path = getAbsoluteRemotePath(remoteRelativePath);
    new File(new File(path).getParent()).mkdirs();
    return new FileInputStream(path);
  }

  @Override
  public OutputStream getOutputStream(String remoteRelativePath) throws IOException {
    String path = getAbsoluteRemotePath(remoteRelativePath);
    new File(new File(path).getParent()).mkdirs();
    return new FileOutputStream(path);
  }

  @Override
  public boolean exists(String remoteRelativePath) throws IOException {
    return new File(getAbsoluteRemotePath(remoteRelativePath)).exists();
  }

  @Override
  public void copyToLocalRoot(String remoteSourceRelativePath, String localDestinationRoot) throws IOException {
    File source = new File(getAbsoluteRemotePath(remoteSourceRelativePath));
    File destination = new File(localDestinationRoot + "/" + source.getName());
    FileUtils.copyFile(source, destination);
  }

  @Override
  public void copyToRemoteRoot(String localSourcePath, String remoteDestinationRelativePath) throws IOException {
    File source = new File(localSourcePath);
    File destination = new File(getAbsoluteRemotePath(remoteDestinationRelativePath));
    FileUtils.copyFile(source, destination);
  }

  @Override
  public boolean attemptDelete(String remoteRelativePath) throws IOException {
    if (exists(remoteRelativePath)) {
      return new File(getAbsoluteRemotePath(remoteRelativePath)).delete();
    } else {
      return false;
    }
  }

  private String getAbsoluteRemotePath(String remoteRelativePath) {
    return partitionRoot + "/" + remoteRelativePath;
  }

  @Override
  public String toString() {
    return "local://" + partitionRoot;
  }
}
