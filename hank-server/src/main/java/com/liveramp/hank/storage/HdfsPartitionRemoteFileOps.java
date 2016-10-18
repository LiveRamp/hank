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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
import com.liveramp.hank.util.IOStreamUtils;

public class HdfsPartitionRemoteFileOps implements PartitionRemoteFileOps {

  private static Logger LOG = LoggerFactory.getLogger(HdfsPartitionRemoteFileOps.class);
  private final boolean useTrash;

  public static enum CompressionCodec {
    GZIP
  }

  public static class Factory implements PartitionRemoteFileOpsFactory {

    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
      return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber, null);
    }
  }

  public static class NoTrashFactory implements PartitionRemoteFileOpsFactory {

    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
      return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber, false);
    }
  }

  public static class GzipFactory implements PartitionRemoteFileOpsFactory {

    @Override
    public PartitionRemoteFileOps getPartitionRemoteFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
      return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber, CompressionCodec.GZIP);
    }
  }

  private final String partitionRoot;
  private final FileSystem fs;
  private final CompressionCodec compressionCodec;

  public HdfsPartitionRemoteFileOps(String remoteDomainRoot,
                                    int partitionNumber) throws IOException {
    this(remoteDomainRoot, partitionNumber, null, true);
  }

  public HdfsPartitionRemoteFileOps(String remoteDomainRoot,
                                    int partitionNumber,
                                    boolean useTrash) throws IOException {
    this(remoteDomainRoot, partitionNumber, null, useTrash);
  }

  public HdfsPartitionRemoteFileOps(String remoteDomainRoot,
                                    int partitionNumber,
                                    CompressionCodec compressionCodec) throws IOException {
    this(remoteDomainRoot, partitionNumber, compressionCodec, true);

  }

  public HdfsPartitionRemoteFileOps(String remoteDomainRoot,
                                    int partitionNumber,
                                    CompressionCodec compressionCodec,
                                    boolean useTrash) throws IOException {
    this.useTrash = useTrash;
    this.partitionRoot = remoteDomainRoot + "/" + partitionNumber;
    Path partitionRootPath = new Path(partitionRoot);
    this.fs = FileSystemHelper.getFileSystemForPath(remoteDomainRoot);
    if (!partitionRootPath.isAbsolute()) {
      throw new IOException("Cannot initialize " + this.getClass().getSimpleName()
          + " with a non absolute remote partition root: "
          + partitionRoot);
    }
    this.compressionCodec = compressionCodec;
  }

  @Override
  public InputStream getInputStream(String remoteRelativePath) throws IOException {
    InputStream inputStream = fs.open(new Path(getRemoteAbsolutePath(remoteRelativePath)));
    if (compressionCodec == null) {
      return inputStream;
    } else {
      switch (compressionCodec) {
        case GZIP:
          return new GZIPInputStream(inputStream);
        default:
          throw new RuntimeException("Compression codec not supported: " + compressionCodec);
      }
    }
  }

  @Override
  public OutputStream getOutputStream(String remoteRelativePath) throws IOException {
    OutputStream outputStream = fs.create(new Path(getRemoteAbsolutePath(remoteRelativePath)), false);
    if (compressionCodec == null) {
      return outputStream;
    } else {
      switch (compressionCodec) {
        case GZIP:
          return new GZIPOutputStream(outputStream);
        default:
          throw new RuntimeException("Compression codec not supported: " + compressionCodec);
      }
    }
  }

  @Override
  public boolean exists(String remoteRelativePath) throws IOException {
    return fs.exists(new Path(getRemoteAbsolutePath(remoteRelativePath)));
  }

  @Override
  public void copyToLocalRoot(String remoteSourceRelativePath, String localDestinationRoot) throws IOException {
    Path source = new Path(getRemoteAbsolutePath(remoteSourceRelativePath));
    File destination = new File(localDestinationRoot + "/" + new Path(remoteSourceRelativePath).getName());
    LOG.info("Copying remote file " + source + " to local file " + destination);
    InputStream inputStream = getInputStream(remoteSourceRelativePath);
    FileOutputStream fileOutputStream = new FileOutputStream(destination);
    try {
      IOStreamUtils.copy(inputStream, fileOutputStream);
      fileOutputStream.flush();
    } finally {
      inputStream.close();
      fileOutputStream.close();
    }
  }

  @Override
  public boolean attemptDelete(String remoteRelativePath) throws IOException {
    if (exists(remoteRelativePath)) {
      Path pathToDelete = new Path(getRemoteAbsolutePath(remoteRelativePath));
      if (useTrash) {
        TrashHelper.deleteUsingTrashIfEnabled(fs, pathToDelete);
      } else {
        fs.delete(pathToDelete, true);
      }
    }
    return true;
  }

  @Override
  public String getRemoteAbsolutePath(String relativePath) {
    if (compressionCodec == null) {
      return partitionRoot + "/" + relativePath;
    } else {
      return partitionRoot + "/" + relativePath + "." + getCompressionCodecExtension(compressionCodec);
    }
  }

  private static String getCompressionCodecExtension(CompressionCodec compressionCodec) {
    switch (compressionCodec) {
      case GZIP:
        return "gz";
      default:
        throw new RuntimeException("Compression codec not supported: " + compressionCodec);
    }
  }

  public static String getRemoteAbsolutePath(String remoteDomainRoot,
                                             int partitionNumber,
                                             String relativePath,
                                             CompressionCodec compressionCodec) throws IOException {
    return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber, compressionCodec).getRemoteAbsolutePath(relativePath);
  }

  public static String getRemoteAbsolutePath(String remoteDomainRoot,
                                             int partitionNumber,
                                             String relativePath) throws IOException {
    return new HdfsPartitionRemoteFileOps(remoteDomainRoot, partitionNumber).getRemoteAbsolutePath(relativePath);
  }

  @Override
  public String toString() {
    return partitionRoot;
  }
}
