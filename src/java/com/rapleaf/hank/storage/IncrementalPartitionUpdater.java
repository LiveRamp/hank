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
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public abstract class IncrementalPartitionUpdater implements PartitionUpdater {

  public static final String FETCH_ROOT_PREFIX = "_fetch_";
  public static final String CACHE_ROOT_NAME = "cache";

  protected final Domain domain;
  protected final String localPartitionRoot;
  protected final String localPartitionRootCache;

  public IncrementalPartitionUpdater(Domain domain,
                                     String localPartitionRoot) throws IOException {
    this.domain = domain;
    this.localPartitionRoot = localPartitionRoot;
    this.localPartitionRootCache = localPartitionRoot + "/" + CACHE_ROOT_NAME;
  }

  /**
   * @return The current valid version number or null if there is none
   * @throws IOException
   */
  protected abstract Integer detectCurrentVersionNumber() throws IOException;

  protected abstract DomainVersion getParentDomainVersion(DomainVersion domainVersion) throws IOException;

  protected abstract Set<DomainVersion> detectCachedVersionsCore() throws IOException;

  protected abstract void cleanCachedVersions() throws IOException;

  protected abstract void fetchVersion(DomainVersion version, String fetchRoot) throws IOException;

  @Override
  public void updateTo(DomainVersion updatingToDomainVersion) throws IOException {
    ensureCacheExists();
    DomainVersion currentVersion = detectCurrentVersion();
    Set<DomainVersion> cachedVersions = detectCachedVersions();
    Set<DomainVersion> versionsNeededToUpdate =
        getVersionsNeededToUpdate(currentVersion, cachedVersions, updatingToDomainVersion);

    // If we don't need any version to update we are done
    if (versionsNeededToUpdate.isEmpty()) {
      return;
    }
    try {
      // Fetch and cache needed versions
      cacheVersionsNeededToUpdate(currentVersion, cachedVersions, versionsNeededToUpdate);

      // Run update based on needed versions in a workspace
      // Move current to cache?
      // Commit workspace

    } finally {
      cleanCachedVersions();
    }
  }

  private File createFetchRoot() throws IOException {
    String fetchRoot = localPartitionRoot + "/" + FETCH_ROOT_PREFIX + UUID.randomUUID().toString();
    File fetchRootFile = new File(fetchRoot);
    if (!fetchRootFile.mkdir()) {
      throw new IOException("Failed to create fetch root: " + fetchRoot);
    }
    return fetchRootFile;
  }

  private void deleteFetchRoots() throws IOException {
    for (File file : new File(localPartitionRoot).listFiles()) {
      if (file.isDirectory() && file.getName().startsWith(FETCH_ROOT_PREFIX)) {
        FileUtils.deleteDirectory(file);
      }
    }
  }

  public Set<DomainVersion> detectCachedVersions() throws IOException {
    ensureCacheExists();
    return detectCachedVersionsCore();
  }

  // Fetch required versions and commit them to cache upon successful fetch
  protected void cacheVersionsNeededToUpdate(DomainVersion currentVersion,
                                             Set<DomainVersion> cachedVersions,
                                             Set<DomainVersion> versionsNeededToUpdate) throws IOException {
    try {
      ensureCacheExists();
      // Clean all previous fetch roots
      deleteFetchRoots();
      // Create new fetch root
      File fetchRoot = createFetchRoot();
      // Fetch versions
      for (DomainVersion version : versionsNeededToUpdate) {
        // Do not fetch current version
        if (currentVersion != null && currentVersion.equals(version)) {
          continue;
        }
        // Do not fetch cached versions
        if (cachedVersions.contains(version)) {
          continue;
        }
        fetchVersion(version, fetchRoot.getPath());
      }
      // Commit fetched versions to cache
      for (File file : fetchRoot.listFiles()) {
        File targetFile = new File(localPartitionRootCache + "/" + file.getName());
        // If target file already exists, delete it
        if (targetFile.exists()) {
          if (!targetFile.delete()) {
            throw new IOException("Failed to overwrite file in cache: " + targetFile.getPath());
          }
        }
        // Move file to final destination in cache
        if (!file.renameTo(targetFile)) {
          throw new IOException("Failed to rename fetched file: " + file.getPath()
              + " to cached file: " + targetFile.getPath());
        }
      }
    } finally {
      // Always delete fetch roots
      deleteFetchRoots();
    }
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

  private DomainVersion detectCurrentVersion() throws IOException {
    Integer currentVersionNumber = detectCurrentVersionNumber();
    if (currentVersionNumber != null) {
      return domain.getVersionByNumber(currentVersionNumber);
    } else {
      return null;
    }
  }
}
