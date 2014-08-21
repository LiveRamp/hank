/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;

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
  }

  @Override
  public InputStream getInputStream(String remoteRelativePath) throws IOException {
    String path = getRemoteAbsolutePath(remoteRelativePath);
    new File(new File(path).getParent()).mkdirs();
    return new FileInputStream(path);
  }

  @Override
  public OutputStream getOutputStream(String remoteRelativePath) throws IOException {
    String path = getRemoteAbsolutePath(remoteRelativePath);
    new File(new File(path).getParent()).mkdirs();
    return new FileOutputStream(path);
  }

  @Override
  public boolean exists(String remoteRelativePath) throws IOException {
    return new File(getRemoteAbsolutePath(remoteRelativePath)).exists();
  }

  @Override
  public void copyToLocalRoot(String remoteSourceRelativePath, String localDestinationRoot) throws IOException {
    File source = new File(getRemoteAbsolutePath(remoteSourceRelativePath));
    File destination = new File(localDestinationRoot + "/" + source.getName());
    FileUtils.copyFile(source, destination);
  }

  @Override
  public boolean attemptDelete(String remoteRelativePath) throws IOException {
    if (exists(remoteRelativePath)) {
      return new File(getRemoteAbsolutePath(remoteRelativePath)).delete();
    } else {
      return false;
    }
  }

  @Override
  public String getRemoteAbsolutePath(String remoteRelativePath) {
    return partitionRoot + "/" + remoteRelativePath;
  }

  @Override
  public String toString() {
    return "local://" + partitionRoot;
  }
}
