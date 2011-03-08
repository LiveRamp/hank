package com.rapleaf.hank.storage.curly;

import java.util.List;

import com.rapleaf.hank.storage.cueball.IFileSelector;

public class CurlyFileSelector implements IFileSelector {

  @Override
  public boolean isRelevantFile(String fileName, Integer fromVersion,
      int toVersion) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> selectFilesToCopy(List<String> relevantFiles,
      Integer fromVersion, int toVersion) {
    // TODO Auto-generated method stub
    return null;
  }

}
