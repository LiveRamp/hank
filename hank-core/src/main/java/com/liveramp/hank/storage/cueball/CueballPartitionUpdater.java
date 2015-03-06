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

package com.liveramp.hank.storage.cueball;

import com.liveramp.hank.compression.cueball.CueballCompressionCodec;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.partition_server.PartitionUpdateTaskStatistics;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.incremental.IncrementalPartitionUpdater;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlan;
import com.liveramp.hank.util.HankTimer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class CueballPartitionUpdater extends IncrementalPartitionUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(CueballPartitionUpdater.class);

  private final PartitionRemoteFileOps partitionRemoteFileOps;
  private final int keyHashSize;
  private final int valueSize;
  private final ICueballMerger cueballMerger;
  private final CueballCompressionCodec compressionCodec;
  private final int hashIndexBits;

  public CueballPartitionUpdater(Domain domain,
                                 PartitionRemoteFileOps partitionRemoteFileOps,
                                 ICueballMerger cueballMerger,
                                 int keyHashSize,
                                 int valueSize,
                                 int hashIndexBits,
                                 CueballCompressionCodec compressionCodec,
                                 String localPartitionRoot) throws IOException {
    super(domain, localPartitionRoot, new CueballUpdatePlanner(domain));
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

  public static boolean isEmptyVersion(PartitionRemoteFileOps partitionRemoteFileOps,
                                       DomainVersion domainVersion) throws IOException {
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
      DomainVersion version = domain.getVersion(file.getVersion());
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
    String fileToFetch = Cueball.getName(domainVersion);
    LOG.info("Fetching from " + partitionRemoteFileOps + " file " + fileToFetch + " to " + fetchRoot);
    partitionRemoteFileOps.copyToLocalRoot(fileToFetch, fetchRoot);
  }

  @Override
  protected void runUpdateCore(DomainVersion currentVersion,
                               DomainVersion updatingToVersion,
                               IncrementalUpdatePlan updatePlan,
                               String updateWorkRoot,
                               PartitionUpdateTaskStatistics statistics) throws IOException {
    runUpdateCore(
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
        null,
        statistics);
  }

  public static void runUpdateCore(DomainVersion currentVersion,
                                   DomainVersion updatingToVersion,
                                   IncrementalUpdatePlan updatePlan,
                                   String updateWorkRoot,
                                   String localPartitionRoot,
                                   String localPartitionRootCache,
                                   ICueballMerger cueballMerger,
                                   int keyHashSize,
                                   int valueSize,
                                   int hashIndexBits,
                                   CueballCompressionCodec compressionCodec,
                                   ValueTransformer valueTransformer,
                                   PartitionUpdateTaskStatistics statistics) throws IOException {

    // Determine new base path
    String newBasePath = updateWorkRoot + "/"
        + Cueball.getName(updatingToVersion.getVersionNumber(), true);

    // Determine files from versions
    CueballFilePath base = getCueballFilePathForVersion(updatePlan.getBase(), currentVersion,
        localPartitionRoot, localPartitionRootCache, true);
    List<CueballFilePath> deltas = new ArrayList<CueballFilePath>();
    for (DomainVersion delta : updatePlan.getDeltasOrdered()) {
      deltas.add(getCueballFilePathForVersion(delta, currentVersion,
          localPartitionRoot, localPartitionRootCache, false));
    }

    // Check that all required files are available
    checkRequiredFileExists(base.getPath());
    for (CueballFilePath delta : deltas) {
      checkRequiredFileExists(delta.getPath());
    }

    HankTimer timer = new HankTimer();
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
    statistics.getDurationsMs().put("Cueball merge", timer.getDurationMs());
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
