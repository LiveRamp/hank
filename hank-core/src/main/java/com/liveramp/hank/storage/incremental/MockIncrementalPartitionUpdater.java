/**
 *  Copyright 2011 LiveRamp
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
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.partition_server.PartitionUpdateTaskStatistics;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MockIncrementalPartitionUpdater extends IncrementalPartitionUpdater {

  private final Domain domain;
  private final Integer currentVersion;
  private final Integer cachedBase;
  private final Integer cachedDelta;

  public MockIncrementalPartitionUpdater(final String localPartitionRoot,
                                         final Domain domain,
                                         final Integer currentVersion) throws IOException {
    this(localPartitionRoot,
        domain,
        currentVersion,
        null,
        null);
  }

  public MockIncrementalPartitionUpdater(final String localPartitionRoot,
                                         final Domain domain,
                                         final Integer currentVersion,
                                         final Integer cachedBase,
                                         final Integer cachedDelta) throws IOException {
    super(domain, localPartitionRoot, null);
    this.domain = domain;
    this.currentVersion = currentVersion;
    this.cachedBase = cachedBase;
    this.cachedDelta = cachedDelta;
  }

  @Override
  protected Set<DomainVersion> detectCachedBasesCore() throws IOException {
    Set<DomainVersion> cachedVersionsSet = new HashSet<DomainVersion>();
    for (Integer versionNumber : Collections.singleton(cachedBase)) {
      if (versionNumber != null) {
        cachedVersionsSet.add(new MockDomainVersion(versionNumber, 0l));
      }
    }
    return cachedVersionsSet;
  }

  @Override
  protected Set<DomainVersion> detectCachedDeltasCore() throws IOException {
    Set<DomainVersion> cachedVersionsSet = new HashSet<DomainVersion>();
    for (Integer versionNumber : Collections.singleton(cachedDelta)) {
      if (versionNumber != null) {
        cachedVersionsSet.add(new MockDomainVersion(versionNumber, 0l));
      }
    }
    return cachedVersionsSet;
  }

  @Override
  protected void cleanCachedVersions() throws IOException {
  }

  @Override
  protected void fetchVersion(DomainVersion version, String fetchRoot) {
  }

  @Override
  protected Integer detectCurrentVersionNumber() throws IOException {
    return currentVersion;
  }

  @Override
  protected void runUpdateCore(DomainVersion currentVersion,
                               DomainVersion updatingToVersion,
                               IncrementalUpdatePlan updatePlan,
                               String updateWorkRoot,
                               PartitionUpdateTaskStatistics statistics) {
  }
}
