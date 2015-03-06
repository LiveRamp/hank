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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.DomainVersions;
import com.liveramp.hank.storage.RemoteDomainCleaner;
import com.liveramp.hank.storage.RemoteDomainVersionDeleter;

public abstract class IncrementalRemoteDomainCleaner implements RemoteDomainCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalRemoteDomainCleaner.class);

  private final Domain domain;
  private final int numRemoteLeafVersionsToKeep;

  public IncrementalRemoteDomainCleaner(Domain domain, int numRemoteLeafVersionsToKeep) {
    this.domain = domain;
    this.numRemoteLeafVersionsToKeep = numRemoteLeafVersionsToKeep;
  }

  protected abstract DomainVersion getParentDomainVersion(Domain domain, DomainVersion domainVersion)
      throws IOException;

  @Override
  public void deleteOldVersions(RemoteDomainVersionDeleter remoteDomainVersionDeleter, boolean deleteMetadata) throws IOException {
    if (numRemoteLeafVersionsToKeep <= 0) {
      return;
    }

    LOG.info("Deleting remote old versions of Domain " + domain.getName()
        + " (keeping " + numRemoteLeafVersionsToKeep + " newest leaf versions)");

    SortedSet<DomainVersion> sortedVersions = new TreeSet<DomainVersion>();
    for (DomainVersion version : domain.getVersions()) {
      if (DomainVersions.isClosed(version)) {
        sortedVersions.add(version);
      }
    }

    // Cache parents
    HashMap<DomainVersion, DomainVersion> cachedParents = new HashMap<DomainVersion, DomainVersion>();
    for (DomainVersion domainVersion : sortedVersions) {
      cachedParents.put(domainVersion, getParentDomainVersion(domain, domainVersion));
    }

    // Determine what versions to keep
    SortedSet<DomainVersion> versionsToKeep = new TreeSet<DomainVersion>();
    for (int i = 0; !sortedVersions.isEmpty() && i < numRemoteLeafVersionsToKeep; ++i) {
      // Keep the next latest leaf version
      DomainVersion version = sortedVersions.last();
      // Backtrack the ancestry of the latest version to know what versions need to be kept
      Set<DomainVersion> ancestry = new HashSet<DomainVersion>();
      // A version is part of its own ancestry
      ancestry.add(version);
      while (true) {
        DomainVersion parent = cachedParents.get(version);
        if (parent == null) {
          break;
        } else {
          ancestry.add(parent);
          version = parent;
        }
      }
      // Keep ancestry
      versionsToKeep.addAll(ancestry);
      // Remove ancestry from versions to examine. Note: since the version being kept is contained in the ancestry,
      // the next iteration will advance to the next last (most recent) version.
      sortedVersions.removeAll(ancestry);
    }

    // Note: all versions to keep have been removed from sortedVersions, so sortedVersions is exactly the set of
    // versions we want to delete.

    // Perform deletion
    for (DomainVersion version : versionsToKeep) {
      LOG.info("Keeping Version " + version.getVersionNumber() + " of Domain " + domain.getName());
    }
    for (DomainVersion version : sortedVersions) {
      // Do not delete defunct versions
      if (!version.isDefunct()) {
        LOG.info("Deleting Version " + version.getVersionNumber() + " of Domain " + domain.getName());
        //TODO: not attempt to delete already deleted versions
        // Set defunct
        version.setDefunct(true);
        // Delete remote data
        remoteDomainVersionDeleter.deleteVersion(version.getVersionNumber());
        // Delete metadata
        if (deleteMetadata) {
          domain.deleteVersion(version.getVersionNumber());
        }
      }
    }
  }
}
