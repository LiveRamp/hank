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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HdfsPartitionRemoteFileOps implements PartitionRemoteFileOps {

  private static Logger LOG = Logger.getLogger(HdfsPartitionRemoteFileOps.class);

  public static class Factory implements PartitionRemoteFileOpsFactory {

    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
      return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber);
    }
  }

  protected final String partitionRoot;
  protected final Path partitionRootPath;
  protected final FileSystem fs;
  protected final String remoteFsUserName;
  protected final String remoteFsGroupName;

  public HdfsPartitionRemoteFileOps(String remoteDomainRoot,
                                    int partitionNumber,
                                    String remoteFsUserName,
                                    String remoteFsGroupName) throws IOException {
    this.partitionRoot = remoteDomainRoot + "/" + partitionNumber;
    this.partitionRootPath = new Path(partitionRoot);
    this.fs = FileSystem.get(new Configuration());
    if (!partitionRootPath.isAbsolute()) {
      throw new IOException("Cannot initialize " + this.getClass().getSimpleName()
          + " with a non absolute remote partition root: "
          + partitionRoot);
    }
    this.remoteFsUserName = remoteFsUserName;
    this.remoteFsGroupName = remoteFsGroupName;
  }

  public HdfsPartitionRemoteFileOps(String remoteDomainRoot,
                                    int partitionNumber) throws IOException {
    this(remoteDomainRoot, partitionNumber, null, null);
  }

  @Override
  public InputStream getInputStream(String remoteRelativePath) throws IOException {
    return fs.open(new Path(getAbsoluteRemotePath(remoteRelativePath)));
  }

  @Override
  public OutputStream getOutputStream(String remoteRelativePath) throws IOException {
    return fs.create(new Path(getAbsoluteRemotePath(remoteRelativePath)), false);
  }

  @Override
  public boolean exists(String remoteRelativePath) throws IOException {
    return fs.exists(new Path(getAbsoluteRemotePath(remoteRelativePath)));
  }

  @Override
  public void copyToLocalRoot(String remoteSourceRelativePath, String localDestinationRoot) throws IOException {
    Path source = new Path(getAbsoluteRemotePath(remoteSourceRelativePath));
    Path destination = new Path(localDestinationRoot + "/" + source.getName());
    LOG.info("Copying remote file " + source + " to local file " + destination);
    fs.copyToLocalFile(source, destination);
  }

  @Override
  public void copyToRemoteRoot(String localSourcePath, String remoteDestinationRelativePath) throws IOException {
    Path source = new Path(localSourcePath);
    Path destination = new Path(getAbsoluteRemotePath(remoteDestinationRelativePath));
    LOG.info("Copying local file " + source + " to remote file " + destination);
    fs.copyFromLocalFile(source, destination);
    if (remoteFsUserName != null || remoteFsGroupName != null) {
      LOG.info("Changing owner of " + destination + " to " + remoteFsUserName + "," + remoteFsGroupName);
      fs.setOwner(destination, remoteFsUserName, remoteFsGroupName);
      fs.setOwner(partitionRootPath, remoteFsUserName, remoteFsGroupName);
    }
  }

  @Override
  public boolean attemptDelete(String remoteRelativePath) throws IOException {
    if (exists(remoteRelativePath)) {
      fs.delete(new Path(getAbsoluteRemotePath(remoteRelativePath)), true);
    }
    return true;
  }

  protected String getAbsoluteRemotePath(String relativePath) {
    return partitionRoot + "/" + relativePath;
  }

  public static String getAbsoluteRemotePath(String remoteDomainRoot, int partitionNumber, String relativePath) throws IOException {
    return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber).getAbsoluteRemotePath(relativePath);
  }

  @Override
  public String toString() {
    return "hdfs://" + partitionRoot;
  }
}
