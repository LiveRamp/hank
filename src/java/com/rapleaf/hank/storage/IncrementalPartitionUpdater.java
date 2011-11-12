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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class IncrementalPartitionUpdater implements PartitionUpdater {

  public static final String CACHE_ROOT_NAME = "cache";

  protected final Domain domain;
  protected final PartitionRemoteFileOps partitionRemoteFileOps;
  protected final String localPartitionRoot;
  protected final String localPartitionRootCache;

  public IncrementalPartitionUpdater(Domain domain,
                                     PartitionRemoteFileOps partitionRemoteFileOps,
                                     String localPartitionRoot) throws IOException {
    this.domain = domain;
    this.partitionRemoteFileOps = partitionRemoteFileOps;
    this.localPartitionRoot = localPartitionRoot;
    this.localPartitionRootCache = localPartitionRoot + "/" + CACHE_ROOT_NAME;
    ensureCacheExists();
  }

  @Override
  public void updateTo(DomainVersion updatingToDomainVersion) throws IOException {
    ensureCacheExists();
    DomainVersion currentVersion = detectCurrentVersion();
    Set<DomainVersion> cachedVersions = getCachedVersions();
    Set<DomainVersion> versionsNeededToUpdate =
        getVersionsNeededToUpdate(currentVersion, cachedVersions, updatingToDomainVersion);

    // If we don't need any version to update we are done
    if (versionsNeededToUpdate.isEmpty()) {
      return;
    }
    // Fetch needed versions in the cache, except current version
    // Run update based on needed versions in a workspace
    // Move current to cache?
    // Commit workspace
    // Clean cache
    throw new NotImplementedException();
  }

  private void ensureCacheExists() throws IOException {
    // Create cache directory if it doesn't exist
    File cacheRootFile = new File(localPartitionRootCache);
    if (!cacheRootFile.exists()) {
      if (!cacheRootFile.mkdir()) {
        throw new IOException("Failed to create cache root directory: " + cacheRootFile.getPath());
      }
    }
  }

  /**
   * Return the set of versions needed to update to the specific version given that
   * the specified current version and cached versions are available.
   */
  protected Set<DomainVersion> getVersionsNeededToUpdate(DomainVersion currentVersion,
                                                         Set<DomainVersion> cachedVersions,
                                                         DomainVersion updatingToVersion) throws IOException {
    Set<DomainVersion> versionsNeeded = new HashSet<DomainVersion>();
    // Backtrack versions (ignoring defunct versions) until we find:
    // - a base (no parent)
    // - or the current version
    // - or a version that is cached
    DomainVersion parentVersion = updatingToVersion;
    while (parentVersion != null) {
      // Ignore completely defunct versions
      if (!parentVersion.isDefunct()) {
        // If a version along the path is still open, abort
        if (!DomainVersions.isClosed(parentVersion)) {
          throw new IOException("Detected a domain version that is still open"
              + " along the path from current version to version to update to:"
              + " open version: " + parentVersion
              + " current version: " + currentVersion
              + " updating to version: " + updatingToVersion);
        }
        // If backtrack to current version, use it and stop backtracking
        if (currentVersion != null && parentVersion.equals(currentVersion)) {
            // If we only need the current version, we don't need any version
          if (versionsNeeded.isEmpty()) {
            return Collections.emptySet();
          } else {
            versionsNeeded.add(parentVersion);
            break;
          }
        }
        // If backtrack to cached version, use it and stop backtracking
        if (cachedVersions.contains(parentVersion)) {
          versionsNeeded.add(parentVersion);
          break;
        }
        // Add backtracked version to versions needed
        versionsNeeded.add(parentVersion);
      }
      // Move to parent version
      parentVersion = getParentDomainVersion(parentVersion);
    }
    return versionsNeeded;
  }

  protected abstract Set<DomainVersion> getCachedVersions() throws IOException;

  private DomainVersion detectCurrentVersion() throws IOException {
    Integer currentVersionNumber = detectCurrentVersionNumber();
    if (currentVersionNumber != null) {
      return domain.getVersionByNumber(currentVersionNumber);
    } else {
      return null;
    }
  }

  /**
   * @return The current valid version number or null if there is none
   * @throws IOException
   */
  protected abstract Integer detectCurrentVersionNumber() throws IOException;

  protected abstract DomainVersion getParentDomainVersion(DomainVersion domainVersion) throws IOException;
}
