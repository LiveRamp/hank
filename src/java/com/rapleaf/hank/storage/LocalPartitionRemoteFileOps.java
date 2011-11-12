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

package com.rapleaf.hank.storage;

import java.io.File;
import java.io.IOException;

public class LocalPartitionRemoteFileOps implements PartitionRemoteFileOps {

  private final String remoteDomainRoot;
  private final int partitionNumber;

  public LocalPartitionRemoteFileOps(String remoteDomainRoot,
                                     int partitionNumber) throws IOException {
    this.remoteDomainRoot = remoteDomainRoot;
    this.partitionNumber = partitionNumber;
  }

  @Override
  public boolean exists(String relativePath) throws IOException {
    return new File(getAbsolutePath(relativePath)).exists();
  }

  @Override
  public void copyToLocalRoot(String relativePath, String localRoot) throws IOException {
    File source = new File(getAbsolutePath(relativePath));
    File destination = new File(localRoot + "/" + source.getName());
    if (!source.renameTo(destination)) {
      throw new IOException("Failed to copy " + source.getParent() + " to " + destination);
    }
  }

  private String getAbsolutePath(String relativeFilePath) {
    return remoteDomainRoot + "/" + partitionNumber + "/" + relativeFilePath;
  }
}
