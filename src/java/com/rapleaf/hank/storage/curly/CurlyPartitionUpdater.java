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

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.IncrementalPartitionUpdater;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.cueball.Cueball;
import com.rapleaf.hank.storage.cueball.CueballFilePath;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

public class CurlyPartitionUpdater extends IncrementalPartitionUpdater {

  public CurlyPartitionUpdater(Domain domain,
                               PartitionRemoteFileOps partitionRemoteFileOps,
                               String localPartitionRoot) throws IOException {
    super(domain, partitionRemoteFileOps, localPartitionRoot);
  }

  @Override
  protected Set<DomainVersion> getCachedVersions() throws IOException {
    // Record in a set all cached Cueball bases
    SortedSet<CueballFilePath> cachedCueballBases = Cueball.getBases(localPartitionRootCache);
    HashSet<Integer> cachedCueballBasesVersions = new HashSet<Integer>();
    for (CueballFilePath cueballCachedVersion : cachedCueballBases) {
      cachedCueballBasesVersions.add(cueballCachedVersion.getVersion());
    }
    // Compute cached Curly bases
    SortedSet<CurlyFilePath> cachedCurlyBases = Curly.getBases(localPartitionRootCache);
    Set<DomainVersion> cachedVersions = new HashSet<DomainVersion>();
    for (CurlyFilePath cachedCurlyBase : cachedCurlyBases) {
      // Check that the corresponding Cueball version is also cached
      if (cachedCueballBasesVersions.contains(cachedCurlyBase.getVersion())) {
        DomainVersion version = domain.getVersionByNumber(cachedCurlyBase.getVersion());
        if (version != null) {
          cachedVersions.add(version);
        }
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
  protected Integer detectCurrentVersionNumber() throws IOException {
    SortedSet<CueballFilePath> localCueballBases = Cueball.getBases(localPartitionRoot);
    SortedSet<CurlyFilePath> localCurlyBases = Curly.getBases(localPartitionRoot);
    if (localCueballBases.size() > 0 && localCurlyBases.size() > 0) {
      if (localCueballBases.last().getVersion() == localCurlyBases.last().getVersion()) {
        return localCurlyBases.last().getVersion();
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  // TODO: determining the parent domain version should be based on DomainVersion metadata instead
  @Override
  protected DomainVersion getParentDomainVersion(DomainVersion domainVersion) throws IOException {
    if (partitionRemoteFileOps.exists(Cueball.getName(domainVersion.getVersionNumber(), true))
        && partitionRemoteFileOps.exists(Curly.getName(domainVersion.getVersionNumber(), true))) {
      // Base files exists, there is no parent
      return null;
    } else if (partitionRemoteFileOps.exists(Cueball.getName(domainVersion.getVersionNumber(), false))
        && partitionRemoteFileOps.exists(Curly.getName(domainVersion.getVersionNumber(), false))) {
      // Delta files exists, the parent is just the previous version based on version number
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
}
