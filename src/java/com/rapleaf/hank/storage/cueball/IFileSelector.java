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

import java.util.List;

/**
 * A File Selector is a bit of code that Fetcher uses to determine which remote
 * files should be copied to the local disk.
 */
public interface IFileSelector {
  /**
   * Is this file a file that we might want to copy? For instance, does it have
   * the right file extension?
   * 
   * @param fileName
   * @param fromVersion
   * @param toVersion
   * @return
   */
  public boolean isRelevantFile(String fileName, Integer fromVersion, int toVersion);

  /**
   * Given a list of relevant files, which ones should be copied? If 100% of
   * your selection logic can be implemented in isRelevant, then it is
   * acceptable to return the same set of input files as the output.
   * 
   * @param relevantFiles
   * @param fromVersion
   * @param toVersion
   * @return
   */
  public List<String> selectFilesToCopy(List<String> relevantFiles, Integer fromVersion, int toVersion);
}
