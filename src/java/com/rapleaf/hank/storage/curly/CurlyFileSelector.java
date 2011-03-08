package com.rapleaf.hank.storage.curly;

import java.util.List;

import com.rapleaf.hank.storage.cueball.CueballFileSelector;

public class CurlyFileSelector extends CueballFileSelector {
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
