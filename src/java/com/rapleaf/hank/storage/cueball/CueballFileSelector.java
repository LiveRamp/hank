/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.rapleaf.hank.storage.cueball;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.rapleaf.hank.storage.IFileSelector;
import org.apache.log4j.Logger;

public class CueballFileSelector implements IFileSelector {
  private static final Logger LOG = Logger.getLogger(CueballFileSelector.class);

  @Override
  public boolean isRelevantFile(String fileName, Integer fromVersion, int toVersion, Set<Integer> excludeVersions) {
    if (isBase(fileName) || fileName.matches(Cueball.DELTA_REGEX)) {
      int ver = parseVersion(fileName);
      if ((fromVersion == null || ver > fromVersion) && ver <= toVersion && !excludeVersions.contains(ver)) {
        return true;
      }
    } else {
      LOG.trace(String.format("%s is not relevant for update %s -> %s (to cueball)", fileName, fromVersion, toVersion));
    }
    return false;
  }

  @Override
  public List<String> selectFilesToCopy(List<String> relevantFiles, Integer fromVersion, int toVersion, Set<Integer> excludeVersions) {
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
      if (parseVersion(path) >= maxBase && !excludeVersions.contains(parseVersion(path))) {
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
