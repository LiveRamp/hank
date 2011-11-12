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

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.DomainVersions;
import org.apache.commons.lang.NotImplementedException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class IncrementalPartitionUpdater implements PartitionUpdater {

  protected final Domain domain;
  protected final PartitionRemoteFileOps partitionRemoteFileOps;
  protected final String localPartitionRoot;

  public IncrementalPartitionUpdater(Domain domain,
                                     PartitionRemoteFileOps partitionRemoteFileOps,
                                     String localPartitionRoot) {
    this.domain = domain;
    this.partitionRemoteFileOps = partitionRemoteFileOps;
    this.localPartitionRoot = localPartitionRoot;
  }

  @Override
  public void updateTo(DomainVersion domainVersion) throws IOException {
    throw new NotImplementedException();
  }

  /**
   * Return the set of versions needed to update to the specific version given that
   * the specified current version is available.
   */
  protected Set<DomainVersion> getVersionsNeededToUpdate(DomainVersion currentVersion,
                                                         DomainVersion updatingToVersion) throws IOException {
    Set<DomainVersion> domainVersions = new HashSet<DomainVersion>();
    // Backtrack versions until we find a base (no parent)
    // or the current version (when the current version is not defunct)
    DomainVersion parentVersion = updatingToVersion;
    while (!(parentVersion == null ||
        (currentVersion != null && !currentVersion.isDefunct() && parentVersion.equals(currentVersion)))) {
      // If a version along the path is still open, abort
      if (!DomainVersions.isClosed(parentVersion)) {
        throw new IOException("Detected a domain version that is still open"
            + " along the path from current version to version to update to:"
            + " open version: " + parentVersion
            + " current version: " + currentVersion
            + " updating to version: " + updatingToVersion);
      }
      // Only use non defunct versions
      if (!parentVersion.isDefunct()) {
        domainVersions.add(parentVersion);
      }
      // Move to parent version
      parentVersion = getParentDomainVersion(parentVersion);
    }
    // If current version is a parent of the version we are updating to, add it to the set of versions needed
    if (parentVersion != null && currentVersion != null
        && !currentVersion.isDefunct() && parentVersion.equals(currentVersion)) {
      domainVersions.add(currentVersion);
    }
    return domainVersions;
  }

  private DomainVersion detectCurrentVersion() throws IOException {
    Integer currentVersionNumber = detectCurrentVersionNumber();
    if (currentVersionNumber != null) {
      return domain.getVersionByNumber(currentVersionNumber);
    } else {
      return null;
    }
  }

  /**
   *
   * @return The current valid version number or null if there is none
   * @throws IOException
   */
  protected abstract Integer detectCurrentVersionNumber() throws IOException;

  protected abstract DomainVersion getParentDomainVersion(DomainVersion domainVersion) throws IOException;
}
