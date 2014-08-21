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

package com.liveramp.hank.storage.incremental;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.storage.PartitionRemoteFileOps;

import java.io.IOException;
import java.util.List;

public class MockIncrementalUpdatePlanner extends IncrementalUpdatePlanner {

  public MockIncrementalUpdatePlanner(Domain domain) {
    super(domain);
  }

  @Override
  public DomainVersion getParentDomainVersion(DomainVersion domainVersion) throws IOException {
    if (domainVersion.getVersionNumber() == 0) {
      return null;
    } else {
      return domain.getVersion(domainVersion.getVersionNumber() - 1);
    }
  }

  @Override
  public List<String> getRemotePartitionFilePaths(IncrementalUpdatePlan updatePlan, PartitionRemoteFileOps partitionRemoteFileOps) throws IOException {
    return null;
  }
}
