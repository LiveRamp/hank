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

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.IncrementalPartitionUpdater;
import com.rapleaf.hank.storage.IncrementalUpdatePlan;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class CueballPartitionUpdater extends IncrementalPartitionUpdater {

  private static final Logger LOG = Logger.getLogger(CueballPartitionUpdater.class);

  private final PartitionRemoteFileOps partitionRemoteFileOps;
  private final int keyHashSize;
  private final int valueSize;
  private final ICueballMerger cueballMerger;
  private final CompressionCodec compressionCodec;
  private final int hashIndexBits;

  public CueballPartitionUpdater(Domain domain,
                                 PartitionRemoteFileOps partitionRemoteFileOps,
                                 ICueballMerger cueballMerger,
                                 int keyHashSize,
                                 int valueSize,
                                 int hashIndexBits,
                                 CompressionCodec compressionCodec,
                                 String localPartitionRoot) throws IOException {
    super(domain, localPartitionRoot);
    this.partitionRemoteFileOps = partitionRemoteFileOps;
    this.cueballMerger = cueballMerger;
    this.keyHashSize = keyHashSize;
    this.valueSize = valueSize;
    this.hashIndexBits = hashIndexBits;
    this.compressionCodec = compressionCodec;
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
    } else if (partitionRemoteFileOps.exists(Cueball.getName(domainVersion.getVersionNumber(), false))
        || isEmptyVersion(domainVersion)) {
      // Delta file exists, or the version is empty, the parent is just the previous version based on version number
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
      throw new IOException("Failed to determine parent version of domain version " + domainVersion);
    }
  }

  private boolean isEmptyVersion(DomainVersion domainVersion) throws IOException {
    return isEmptyVersion(domainVersion, partitionRemoteFileOps);
  }

  private static boolean isEmptyVersion(DomainVersion domainVersion,
                                        PartitionRemoteFileOps partitionRemoteFileOps) throws IOException {
    return !partitionRemoteFileOps.exists(Cueball.getName(domainVersion.getVersionNumber(), true))
        && !partitionRemoteFileOps.exists(Cueball.getName(domainVersion.getVersionNumber(), false));
  }

  @Override
  protected Set<DomainVersion> detectCachedBasesCore() throws IOException {
    return detectCachedVersions(Cueball.getBases(localPartitionRootCache));
  }

  @Override
  protected Set<DomainVersion> detectCachedDeltasCore() throws IOException {
    return detectCachedVersions(Cueball.getDeltas(localPartitionRootCache));
  }

  private Set<DomainVersion> detectCachedVersions(SortedSet<CueballFilePath> cachedFiles) throws IOException {
    Set<DomainVersion> cachedVersions = new HashSet<DomainVersion>();
    for (CueballFilePath file : cachedFiles) {
      DomainVersion version = domain.getVersionByNumber(file.getVersion());
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
  protected void fetchVersion(DomainVersion domainVersion, String fetchRoot) throws IOException {
    // Determine if version is a base or delta
    // TODO: use version's metadata to determine if it's a base or a delta
    Boolean isBase = null;
    if (partitionRemoteFileOps.exists(Cueball.getName(domainVersion.getVersionNumber(), true))) {
      isBase = true;
    } else if (partitionRemoteFileOps.exists(Cueball.getName(domainVersion.getVersionNumber(), false))) {
      isBase = false;
    }
    if (isBase == null) {
      // If unable to determine if it's a base or delta, do not fetch anything
      return;
    }
    // Fetch version files
    String fileToFetch = Cueball.getName(domainVersion.getVersionNumber(), isBase);
    if (partitionRemoteFileOps.exists(fileToFetch)) {
      LOG.info("Fetching from " + partitionRemoteFileOps + " file " + fileToFetch + " to " + fetchRoot);
      partitionRemoteFileOps.copyToLocalRoot(fileToFetch, fetchRoot);
    }
  }

  @Override
  protected void runUpdateCore(DomainVersion currentVersion,
                               DomainVersion updatingToVersion,
                               IncrementalUpdatePlan updatePlan,
                               String updateWorkRoot) throws IOException {
    runUpdateCore(partitionRemoteFileOps,
        currentVersion,
        updatingToVersion,
        updatePlan,
        updateWorkRoot,
        localPartitionRoot,
        localPartitionRootCache,
        cueballMerger,
        keyHashSize,
        valueSize,
        hashIndexBits,
        compressionCodec,
        null);
  }

  public static void runUpdateCore(PartitionRemoteFileOps partitionRemoteFileOps,
                                   DomainVersion currentVersion,
                                   DomainVersion updatingToVersion,
                                   IncrementalUpdatePlan updatePlan,
                                   String updateWorkRoot,
                                   String localPartitionRoot,
                                   String localPartitionRootCache,
                                   ICueballMerger cueballMerger,
                                   int keyHashSize,
                                   int valueSize,
                                   int hashIndexBits,
                                   CompressionCodec compressionCodec,
                                   ValueTransformer valueTransformer) throws IOException {

    // Determine new base path
    String newBasePath = updateWorkRoot + "/"
        + Cueball.getName(updatingToVersion.getVersionNumber(), true);

    // Determine files from versions
    CueballFilePath base = getCueballFilePathForVersion(updatePlan.getBase(), currentVersion,
        localPartitionRoot, localPartitionRootCache, true);
    List<CueballFilePath> deltas = new ArrayList<CueballFilePath>();
    for (DomainVersion delta : updatePlan.getDeltasOrdered()) {
      // Only add to the delta list if the version is not empty
      if (!isEmptyVersion(delta, partitionRemoteFileOps)) {
        deltas.add(getCueballFilePathForVersion(delta, currentVersion,
            localPartitionRoot, localPartitionRootCache, false));
      }
    }

    // Check that all required files are available
    checkRequiredFileExists(base.getPath());
    for (CueballFilePath delta : deltas) {
      checkRequiredFileExists(delta.getPath());
    }

    // If there are no deltas, simply move the required base to the target version.
    // Otherwise, perform merging.
    if (deltas.size() == 0) {
      if (!new File(base.getPath()).renameTo(new File(newBasePath))) {
        throw new IOException("Failed to rename Cueball base: " + base.getPath() + " to: " + newBasePath);
      }
    } else {
      cueballMerger.merge(base,
          deltas,
          newBasePath,
          keyHashSize,
          valueSize,
          valueTransformer,
          hashIndexBits,
          compressionCodec);
    }
  }

  public CueballFilePath getCueballFilePathForVersion(DomainVersion version,
                                                      DomainVersion currentVersion,
                                                      boolean isBase) {
    return getCueballFilePathForVersion(version,
        currentVersion,
        localPartitionRoot,
        localPartitionRootCache,
        isBase);
  }

  public static CueballFilePath getCueballFilePathForVersion(DomainVersion version,
                                                             DomainVersion currentVersion,
                                                             String localPartitionRoot,
                                                             String localPartitionRootCache,
                                                             boolean isBase) {
    if (currentVersion != null && currentVersion.equals(version)) {
      // If version is current version, data is in root
      return new CueballFilePath(localPartitionRoot + "/" + Cueball.getName(version.getVersionNumber(), isBase));
    } else {
      // Otherwise, version must be in cache
      return new CueballFilePath(localPartitionRootCache + "/" + Cueball.getName(version.getVersionNumber(), isBase));
    }
  }

  public static void checkRequiredFileExists(String path) throws IOException {
    if (!new File(path).exists()) {
      throw new IOException("Could not find required file for merging: " + path);
    }
  }
}
