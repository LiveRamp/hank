package com.liveramp.hank.storage.operations;

import java.io.IOException;
import java.io.InputStream;

public interface PartitionServerRemoteFileOps {

  public void copyToLocalRoot(String remoteSourceRelativePath, String localDestinationRoot) throws IOException;

  public InputStream getInputStream(String remoteRelativePath) throws IOException;

  public boolean exists(String remoteRelativePath) throws IOException;

}
