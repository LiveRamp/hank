package com.rapleaf.hank.storage.cueball;

import java.util.ArrayList;
import java.util.List;

public class CueballFileSelector implements IFileSelector {

  @Override
  public boolean isRelevantFile(String fileName, Integer fromVersion, int toVersion) {
    if (fileName.matches(Cueball.BASE_REGEX) || fileName.matches(Cueball.DELTA_REGEX)) {
      int ver = Cueball.parseVersionNumber(fileName);
      if ((fromVersion == null || ver > fromVersion) && ver <= toVersion) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<String> selectFilesToCopy(List<String> relevantFiles, Integer fromVersion, int toVersion) {
    Integer maxBase = null;
    for (String path : relevantFiles) {
      if (path.matches(Cueball.BASE_REGEX)) {
        if (maxBase == null || maxBase < Cueball.parseVersionNumber(path)) {
          maxBase = Cueball.parseVersionNumber(path);
        }
      }
    }
    if (maxBase == null) {
      return relevantFiles;
    }
    List<String> filesToCopy = new ArrayList<String>(); 
    for (String path : relevantFiles) {
      if (Cueball.parseVersionNumber(path) >= maxBase) {
        filesToCopy.add(path);
      }
    }
    return filesToCopy;
  }
}
