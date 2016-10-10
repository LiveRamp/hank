package com.liveramp.hank.storage.operations;

import java.io.IOException;
import java.io.OutputStream;

public interface DomainBuilderRemoteFileOps {

  public OutputStream getOutputStream(String remoteRelativePath) throws IOException;

  public boolean exists(String remoteRelativePath) throws IOException;

  public boolean attemptDelete(String remoteRelativePath) throws IOException;

  public String getRemoteAbsolutePath(String remoteRelativePath);


}
