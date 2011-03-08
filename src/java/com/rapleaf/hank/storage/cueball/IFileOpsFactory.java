package com.rapleaf.hank.storage.cueball;

public interface IFileOpsFactory {
  public IFileOps getFileOps(String localPath, String remotePath);
}
