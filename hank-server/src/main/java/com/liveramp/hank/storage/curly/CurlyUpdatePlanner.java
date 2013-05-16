/**
 *  Copyright 2013 LiveRamp
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

package com.liveramp.hank.storage.curly;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.cueball.Cueball;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlan;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CurlyUpdatePlanner extends IncrementalUpdatePlanner {

  public CurlyUpdatePlanner(Domain domain) {
    super(domain);
  }

  @Override
  public List<String> getRemotePartitionFilePaths(IncrementalUpdatePlan updatePlan,
                                                  PartitionRemoteFileOps partitionRemoteFileOps) throws IOException {
    List<String> result = new ArrayList<String>();
    for (DomainVersion domainVersion : updatePlan.getAllVersions()) {
      result.add(partitionRemoteFileOps.getRemoteAbsolutePath(Curly.getName(domainVersion)));
      result.add(partitionRemoteFileOps.getRemoteAbsolutePath(Cueball.getName(domainVersion)));
    }
    return result;
  }
}
