package com.rapleaf.hank.storage.cueball;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class CueballFileSelector implements IFileSelector {
  private static final Logger LOG = Logger.getLogger(CueballFileSelector.class);

  @Override
  public boolean isRelevantFile(String fileName, Integer fromVersion, int toVersion) {
    if (isBase(fileName) || fileName.matches(Cueball.DELTA_REGEX)) {
      int ver = parseVersion(fileName);
      if ((fromVersion == null || ver > fromVersion) && ver <= toVersion) {
        return true;
      }
    } else {
      LOG.trace(String.format("%s is not relevant for update %s -> %s (to cueball)", fileName, fromVersion, toVersion));
    }
    return false;
  }

  @Override
  public List<String> selectFilesToCopy(List<String> relevantFiles, Integer fromVersion, int toVersion) {
    Integer maxBase = null;
    for (String path : relevantFiles) {
      if (isBase(path)) {
        if (maxBase == null || maxBase < parseVersion(path)) {
          maxBase = parseVersion(path);
        }
      }
    }
    if (maxBase == null) {
      return relevantFiles;
    }
    List<String> filesToCopy = new ArrayList<String>(); 
    for (String path : relevantFiles) {
      if (parseVersion(path) >= maxBase) {
        filesToCopy.add(path);
      }
    }
    return filesToCopy;
  }

  protected int parseVersion(String path) {
    return Cueball.parseVersionNumber(path);
  }

  protected boolean isBase(String path) {
    return path.matches(Cueball.BASE_REGEX);
  }
}
