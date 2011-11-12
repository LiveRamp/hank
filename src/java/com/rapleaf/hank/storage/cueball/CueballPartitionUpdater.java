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

package com.rapleaf.hank.storage.cueball;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.IncrementalPartitionUpdater;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

public class CueballPartitionUpdater extends IncrementalPartitionUpdater {

  private static final Logger LOG = Logger.getLogger(CueballPartitionUpdater.class);

  private final PartitionRemoteFileOps partitionRemoteFileOps;

  public CueballPartitionUpdater(Domain domain,
                                 PartitionRemoteFileOps partitionRemoteFileOps,
                                 String localPartitionRoot) throws IOException {
    super(domain, localPartitionRoot);
    this.partitionRemoteFileOps = partitionRemoteFileOps;
  }

  @Override
  protected Integer detectCurrentVersionNumber() throws IOException {
    SortedSet<CueballFilePath> localBases = Cueball.getBases(localPartitionRoot);
    if (localBases.size() > 0) {
      return localBases.last().getVersion();
    } else {
      return null;
    }
  }

  // TODO: determining the parent domain version should be based on DomainVersion metadata instead
  @Override
  protected DomainVersion getParentDomainVersion(DomainVersion domainVersion) throws IOException {
    if (partitionRemoteFileOps.exists(Cueball.getName(domainVersion.getVersionNumber(), true))) {
      // Base file exists, there is no parent
      return null;
    } else if (partitionRemoteFileOps.exists(Cueball.getName(domainVersion.getVersionNumber(), false))) {
      // Delta file exists, the parent is just the previous version based on version number
      int versionNumber = domainVersion.getVersionNumber();
      if (versionNumber <= 0) {
        return null;
      } else {
        DomainVersion result = domain.getVersionByNumber(versionNumber - 1);
        if (result == null) {
          throw new IOException("Failed to find version numbered " + (versionNumber - 1)
              + " of domain " + domain
              + " which was determined be the parent of domain version " + domainVersion);
        }
        return result;
      }
    } else {
      throw new IOException("Failed to determine parent version of domain version: " + domainVersion);
    }
  }

  @Override
  protected Set<DomainVersion> detectCachedVersionsCore() throws IOException {
    SortedSet<CueballFilePath> cachedBases = Cueball.getBases(localPartitionRootCache);
    Set<DomainVersion> cachedVersions = new HashSet<DomainVersion>();
    for (CueballFilePath base : cachedBases) {
      DomainVersion version = domain.getVersionByNumber(base.getVersion());
      if (version != null) {
        cachedVersions.add(version);
      }
    }
    return cachedVersions;
  }

  @Override
  protected void cleanCachedVersions() throws IOException {
    // Delete all cached versions
    FileUtils.deleteDirectory(new File(localPartitionRootCache));
  }

  @Override
  protected void fetchVersion(DomainVersion version, String fetchRoot) throws IOException {
    // Determine if version is a base or delta
    // TODO: use version's metadata to determine if it's a base or a delta
    Boolean isBase = null;
    if (partitionRemoteFileOps.exists(Cueball.getName(version.getVersionNumber(), true))) {
      isBase = true;
    } else if (partitionRemoteFileOps.exists(Cueball.getName(version.getVersionNumber(), false))) {
      isBase = false;
    }
    if (isBase == null) {
      throw new IOException("Failed to determine if version was a base or a delta: " + version);
    }
    // Fetch version files
    String fileToFetch = Cueball.getName(version.getVersionNumber(), isBase);
    LOG.info("Fetching: " + fileToFetch + " to: " + fetchRoot);
    partitionRemoteFileOps.copyToLocalRoot(fileToFetch, fetchRoot);
  }
}
