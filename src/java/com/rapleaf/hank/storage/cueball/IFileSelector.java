package com.rapleaf.hank.storage.cueball;

import java.util.List;

public interface IFileSelector {

  public boolean isRelevantFile(String fileName, int fromVersion, int toVersion);

  public List<String> selectFilesToCopy(List<String> relevantFiles);

}
