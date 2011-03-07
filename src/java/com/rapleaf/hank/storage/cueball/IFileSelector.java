package com.rapleaf.hank.storage.cueball;

import java.util.List;

public interface IFileSelector {

  public boolean isRelevantFile(String fileName, Integer fromVersion, int toVersion);

  public List<String> selectFilesToCopy(List<String> relevantFiles, Integer fromVersion, int toVersion);

}
