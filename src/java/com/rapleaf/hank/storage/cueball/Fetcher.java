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

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class Fetcher implements IFetcher {
  private static final Logger LOG = Logger.getLogger(Fetcher.class);

  private final IFileOps fileOps;
  private final IFileSelector fileSelector;

  public Fetcher(IFileOps fileOps, IFileSelector fileSelector) {
    this.fileOps = fileOps;
    this.fileSelector = fileSelector;
  }

  @Override
  public void fetch(int fromVersion,
                    int toVersion,
                    Set<Integer> excludeVersions,
                    String localDirectory) throws IOException {
    List<String> remoteFiles = fileOps.listFiles();
    LOG.debug("Remote files: " + remoteFiles);
    if (remoteFiles != null) {
      List<String> relevantFiles = new ArrayList<String>(remoteFiles.size());
      for (String fileName : remoteFiles) {
        if (fileSelector.isRelevantFile(fileName, fromVersion, toVersion, excludeVersions)) {
          relevantFiles.add(fileName);
        }
      }
      LOG.debug("Relevant files: " + relevantFiles);
      List<String> filesToCopy = fileSelector.selectFilesToCopy(relevantFiles, fromVersion, toVersion, excludeVersions);
      for (String fileName : filesToCopy) {
        LOG.debug("Copying " + fileName + " to " + localDirectory);
        fileOps.copyToLocal(fileName, localDirectory);
      }
    }
  }
}
