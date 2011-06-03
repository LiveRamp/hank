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

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class MockFetcher implements IFetcher {
  public int latestLocalVersion;
  private String[] localFilesToCreate;
  private final String localRoot;
  public Set<Integer> excludeVersions;

  public MockFetcher(String localRoot, String ... localFilesToCreate) {
    this.localRoot = localRoot;
    this.localFilesToCreate = localFilesToCreate;
  }

  @Override
  public void fetch(int fromVersion, int toVersion, Set<Integer> excludeVersions) throws IOException {
    this.latestLocalVersion = fromVersion;
    this.excludeVersions = excludeVersions;
    for (String s : localFilesToCreate) {
      new File(localRoot + "/" + s).createNewFile();
    }
  }
}