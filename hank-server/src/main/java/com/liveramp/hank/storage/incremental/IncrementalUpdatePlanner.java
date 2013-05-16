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
import com.liveramp.hank.coordinator.DomainVersions;
import com.liveramp.hank.storage.PartitionRemoteFileOps;

import java.io.IOException;
import java.util.*;

public abstract class IncrementalUpdatePlanner {

  protected final Domain domain;

  public IncrementalUpdatePlanner(Domain domain) {
    this.domain = domain;
  }

  public IncrementalUpdatePlan computeUpdatePlan(DomainVersion updatingToVersion) throws IOException {
    return computeUpdatePlan(null, new HashSet<DomainVersion>(), updatingToVersion);
  }

  /**
   * Return the list of versions needed to update to the specific version given that
   * the specified current version and cached bases are available.
   *
   * @param currentVersion
   * @param cachedBases
   * @param updatingToVersion
   * @return
   * @throws java.io.IOException
   */
  protected IncrementalUpdatePlan computeUpdatePlan(DomainVersion currentVersion,
                                                    Set<DomainVersion> cachedBases,
                                                    DomainVersion updatingToVersion) throws IOException {
    LinkedList<DomainVersion> updatePlanVersions = new LinkedList<DomainVersion>();
    // Backtrack versions (ignoring defunct versions) until we find:
    // - a base (no parent)
    // - or the current version (which is by definition a base or a rebased delta)
    // - or a version that is a base and that is cached
    DomainVersion parentVersion = updatingToVersion;
    while (parentVersion != null) {
      // Ignore completely defunct versions
      if (!parentVersion.isDefunct()) {
        // If a version along the path is still open, abort
        if (!DomainVersions.isClosed(parentVersion)) {
          throw new IOException("Detected a domain version that is still open"
              + " along the path from current version to version to update to: "
              + " domain: " + domain
              + " open version: " + parentVersion
              + " current version: " + currentVersion
              + " updating to version: " + updatingToVersion);
        }
        // If backtrack to current version, use it and stop backtracking
        if (currentVersion != null && parentVersion.equals(currentVersion)) {
          // If we only need the current version, we don't need any plan
          if (updatePlanVersions.isEmpty()) {
            return null;
          } else {
            updatePlanVersions.add(parentVersion);
            break;
          }
        }
        // If backtrack to cached base version, use it and stop backtracking
        if (cachedBases.contains(parentVersion)) {
          updatePlanVersions.add(parentVersion);
          break;
        }
        // Add backtracked version to versions needed
        updatePlanVersions.add(parentVersion);
      }
      // Move to parent version
      parentVersion = getParentDomainVersion(parentVersion);
    }
    if (updatePlanVersions.isEmpty()) {
      return null;
    }
    // The base is the last version that was added (a base, the current version or a cached base)
    DomainVersion base = updatePlanVersions.removeLast();
    // Check that the base we are going to update from is not invalid
    if (!(getParentDomainVersion(base) == null ||
        cachedBases.contains(base) ||
        (currentVersion != null && base.equals(currentVersion)))) {
      throw new IOException("Failed to find a valid base from which to update: "
          + " domain: " + domain
          + " not a valid base: " + base
          + " current version: " + currentVersion
          + " updating to version: " + updatingToVersion);
    }
    // Reverse list of deltas as we have added versions going backwards
    Collections.reverse(updatePlanVersions);
    return new IncrementalUpdatePlan(base, updatePlanVersions);
  }

  public DomainVersion getParentDomainVersion(DomainVersion domainVersion) throws IOException {
    return IncrementalDomainVersionProperties.getParentDomainVersion(domain, domainVersion);
  }

  public abstract List<String> getRemotePartitionFilePaths(IncrementalUpdatePlan updatePlan,
                                                           PartitionRemoteFileOps partitionRemoteFileOps) throws IOException;
}
