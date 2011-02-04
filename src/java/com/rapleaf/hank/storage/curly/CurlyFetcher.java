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
package com.rapleaf.hank.storage.curly;

import org.apache.hadoop.fs.Path;

import com.rapleaf.hank.storage.cueball.Fetcher;

public class CurlyFetcher extends Fetcher {
  public CurlyFetcher(String localPartitionRoot, String remotePartitionRoot) {
    super(localPartitionRoot, remotePartitionRoot);
  }

  @Override
  protected boolean isRelevantFile(String name) {
    return super.isRelevantFile(name) || name.matches(Curly.BASE_REGEX)
        || name.matches(Curly.DELTA_REGEX);
  }

  @Override
  protected boolean newerThanCutoff(Path p, int cutoffVersionNumber) {
    if (super.newerThanCutoff(p, cutoffVersionNumber)) {
      return true;
    }
    String name = p.getName();
    return ((name.matches(Curly.BASE_REGEX) || name.matches(Curly.DELTA_REGEX)) 
        && Curly.parseVersionNumber(name) >= cutoffVersionNumber);
  }
}
