package com.rapleaf.hank.storage;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class HdfsGzipPartitionRemoteFileOps extends HdfsPartitionRemoteFileOps implements PartitionRemoteFileOps {

  private static Logger LOG = Logger.getLogger(HdfsGzipPartitionRemoteFileOps.class);

  public HdfsGzipPartitionRemoteFileOps(String remoteDomainRoot,
                                        int partitionNumber,
                                        String remoteFsUserName,
                                        String remoteFsGroupName) throws IOException {
    super(remoteDomainRoot, partitionNumber, remoteFsUserName, remoteFsGroupName);
  }

  public HdfsGzipPartitionRemoteFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
    this(remoteDomainRoot, partitionNumber, null, null);
  }

  public static class Factory implements PartitionRemoteFileOpsFactory {
    @Override
    public PartitionRemoteFileOps getFileOps(String remoteDomainRoot, int partitionNumber) throws IOException {
      return new HdfsGzipPartitionRemoteFileOps(remoteDomainRoot, partitionNumber);
    }
  }

  @Override
  public void copyToLocalRoot(String remoteSourceRelativePath, String localDestinationRoot) throws IOException {
    Path source = new Path(getAbsolutePath(remoteSourceRelativePath));
    File destination = new File(localDestinationRoot + "/" + source.getName());
    LOG.info("Copying remote file " + source + " to local file " + destination);
    GZIPInputStream inputStream = new GZIPInputStream(fs.open(source));
    BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(destination));
    // Perform copy using copyLarge() (for files over 2GB)
    IOUtils.copyLarge(inputStream, outputStream);
    outputStream.close();
    inputStream.close();
  }

  @Override
  public void copyToRemoteRoot(String localSourcePath, String remoteDestinationRelativePath) throws IOException {
    //Path source = new Path(localSourcePath);
    File source = new File(localSourcePath);
    Path destination = new Path(getAbsolutePath(remoteDestinationRelativePath));
    LOG.info("Copying local file " + source + " to remote file " + destination);
    // No need to use a BufferedInputStream since IOUtils uses a buffer internally to do the copy.
    FileInputStream inputStream = new FileInputStream(source);
    GZIPOutputStream outputStream = new GZIPOutputStream(fs.create(destination, true));
    // Perform copy using copyLarge() (for files over 2GB)
    IOUtils.copyLarge(inputStream, outputStream);
    outputStream.close();
    inputStream.close();
    // Change ownership if necessary
    if (remoteFsUserName != null || remoteFsGroupName != null) {
      LOG.info("Changing owner of " + destination + " to " + remoteFsUserName + "," + remoteFsGroupName);
      fs.setOwner(destination, remoteFsUserName, remoteFsGroupName);
      fs.setOwner(partitionRootPath, remoteFsUserName, remoteFsGroupName);
    }
  }

  @Override
  protected String getAbsolutePath(String relativePath) {
    return partitionRoot + "/" + relativePath + ".gz";
  }

  @Override
  public String toString() {
    return "hdfs-gzip://" + partitionRoot;
  }
}
