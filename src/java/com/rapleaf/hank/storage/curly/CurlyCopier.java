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

import com.rapleaf.hank.storage.Copier;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;

import java.io.IOException;

public class CurlyCopier implements Copier {

  private final String localDir;
  private final Copier keyFileCopier;

  public CurlyCopier(String localDir, Copier keyFileCopier) {
    this.localDir = localDir;
    this.keyFileCopier = keyFileCopier;
  }

  @Override
  public void copyVersionTo(int versionNumber, PartitionRemoteFileOps partitionRemoteFileOps) throws IOException {
    keyFileCopier.copyVersionTo(versionNumber, partitionRemoteFileOps);
    String fileName = localDir + "/" + Curly.getName(versionNumber, true);
    partitionRemoteFileOps.copyToRemoteRoot(fileName);
  }
}
