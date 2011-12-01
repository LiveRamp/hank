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

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.IncrementalUpdatePlan;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public class CurlyCompactingPartitionUpdater extends AbstractCurlyPartitionUpdater {

  public CurlyCompactingPartitionUpdater(Domain domain,
                                         PartitionRemoteFileOps partitionRemoteFileOps,
                                         int keyHashSize,
                                         int offsetSize,
                                         int hashIndexBits,
                                         CompressionCodec compressionCodec,
                                         String localPartitionRoot) throws IOException {
    super(domain, partitionRemoteFileOps, keyHashSize, offsetSize, hashIndexBits, compressionCodec, localPartitionRoot);
  }

  @Override
  protected void runUpdateCore(DomainVersion currentVersion,
                               DomainVersion updatingToVersion,
                               IncrementalUpdatePlan updatePlan,
                               String updateWorkRoot) throws IOException {
    throw new NotImplementedException();
  }
}
