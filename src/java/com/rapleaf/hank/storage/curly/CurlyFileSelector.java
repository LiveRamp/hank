package com.rapleaf.hank.storage.curly;

import java.util.List;

import org.apache.log4j.Logger;

import com.rapleaf.hank.storage.cueball.CueballFileSelector;

public class CurlyFileSelector extends CueballFileSelector {
  private static final Logger LOG = Logger.getLogger(CurlyFileSelector.class);

  @Override
  public boolean isRelevantFile(String fileName,
      Integer fromVersion,
      int toVersion)
  {
    if (fileName.matches(Curly.BASE_REGEX) || fileName.matches(Curly.DELTA_REGEX)) {
      int ver = Curly.parseVersionNumber(fileName);
      if ((fromVersion == null || ver > fromVersion) && ver <= toVersion) {
        return true;
      }
    } else {
      LOG.trace(String.format("%s is not relevant for update %s -> %s (to curly)", fileName, fromVersion, toVersion));
    }

    return super.isRelevantFile(fileName, fromVersion, toVersion);
  }

  @Override
  public List<String> selectFilesToCopy(List<String> relevantFiles,
      Integer fromVersion,
      int toVersion)
  {
    return super.selectFilesToCopy(relevantFiles, fromVersion, toVersion);
  }

  @Override
  protected boolean isBase(String path) {
    if (path.matches(Curly.BASE_REGEX)) {
      return true;
    }
    return super.isBase(path);
  }

  @Override
  protected int parseVersion(String path) {
    if (path.matches(Curly.BASE_REGEX) || path.matches(Curly.DELTA_REGEX)) {
      return Curly.parseVersionNumber(path);
    }
    return super.parseVersion(path);
  }
}
