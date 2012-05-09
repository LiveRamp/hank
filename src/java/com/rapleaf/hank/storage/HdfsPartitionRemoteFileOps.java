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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;

import java.io.*;

public class HdfsPartitionRemoteFileOps implements PartitionRemoteFileOps {

  private static Logger LOG = Logger.getLogger(HdfsPartitionRemoteFileOps.class);

  public static class Factory implements PartitionRemoteFileOpsFactory {

    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
      return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber, null);
    }
  }

  public static class GzipFactory implements PartitionRemoteFileOpsFactory {

    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
      return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber, new GzipCodec());
    }
  }

  public static class SnappyFactory implements PartitionRemoteFileOpsFactory {

    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
      return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber, new SnappyCodec());
    }
  }

  public static class Bzip2Factory implements PartitionRemoteFileOpsFactory {

    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
      return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber, new BZip2Codec());
    }
  }

  private final String partitionRoot;
  private final FileSystem fs;
  private final CompressionCodec compressionCodec;

  public HdfsPartitionRemoteFileOps(String remoteDomainRoot,
                                    int partitionNumber) throws IOException {
    this(remoteDomainRoot, partitionNumber, null);
  }

  public HdfsPartitionRemoteFileOps(String remoteDomainRoot,
                                    int partitionNumber,
                                    CompressionCodec compressionCodec) throws IOException {
    this.partitionRoot = remoteDomainRoot + "/" + partitionNumber;
    Path partitionRootPath = new Path(partitionRoot);
    this.fs = FileSystem.get(new Configuration());
    if (!partitionRootPath.isAbsolute()) {
      throw new IOException("Cannot initialize " + this.getClass().getSimpleName()
          + " with a non absolute remote partition root: "
          + partitionRoot);
    }
    this.compressionCodec = compressionCodec;
  }

  @Override
  public InputStream getInputStream(String remoteRelativePath) throws IOException {
    InputStream inputStream = fs.open(new Path(getAbsoluteRemotePath(remoteRelativePath)));
    if (compressionCodec == null) {
      return inputStream;
    } else {
      return compressionCodec.createInputStream(inputStream);
    }
  }

  @Override
  public OutputStream getOutputStream(String remoteRelativePath) throws IOException {
    OutputStream outputStream = fs.create(new Path(getAbsoluteRemotePath(remoteRelativePath)), false);
    if (compressionCodec == null) {
      return outputStream;
    } else {
      return compressionCodec.createOutputStream(outputStream);
    }
  }

  @Override
  public boolean exists(String remoteRelativePath) throws IOException {
    return fs.exists(new Path(getAbsoluteRemotePath(remoteRelativePath)));
  }

  @Override
  public void copyToLocalRoot(String remoteSourceRelativePath, String localDestinationRoot) throws IOException {
    Path source = new Path(getAbsoluteRemotePath(remoteSourceRelativePath));
    File destination = new File(localDestinationRoot + "/" + new Path(remoteSourceRelativePath).getName());
    LOG.info("Copying remote file " + source + " to local file " + destination);
    InputStream inputStream = getInputStream(remoteSourceRelativePath);
    // Use copyLarge (over 2GB)
    try {
      IOUtils.copyLarge(inputStream,
          new BufferedOutputStream(new FileOutputStream(destination)));
    } finally {
      inputStream.close();
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
    if (compressionCodec == null) {
      return partitionRoot + "/" + relativePath;
    } else {
      return partitionRoot + "/" + relativePath + "." + compressionCodec.getDefaultExtension();
    }
  }

  public static String getAbsoluteRemotePath(String remoteDomainRoot,
                                             int partitionNumber,
                                             String relativePath,
                                             CompressionCodec compressionCodec) throws IOException {
    return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber, compressionCodec).getAbsoluteRemotePath(relativePath);
  }

  public static String getAbsoluteRemotePath(String remoteDomainRoot,
                                             int partitionNumber,
                                             String relativePath) throws IOException {
    return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber).getAbsoluteRemotePath(relativePath);
  }

  @Override
  public String toString() {
    return "hdfs://" + partitionRoot;
  }
}
