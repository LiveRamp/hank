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

import com.liveramp.hank.coordinator.CloseCoordinatorOpportunistically;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.partition_server.PartitionUpdateTaskStatistics;
import com.liveramp.hank.storage.PartitionUpdater;
import com.liveramp.hank.util.FormatUtils;
import com.liveramp.hank.util.HankTimer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

public abstract class IncrementalPartitionUpdater implements PartitionUpdater, CloseCoordinatorOpportunistically {

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalPartitionUpdater.class);

  public static final String FETCH_ROOT_PREFIX = "_fetch_";
  public static final String UPDATE_WORK_ROOT_PREFIX = "_update_work_";
  public static final String CACHE_ROOT_NAME = "cache";

  protected final Domain domain;
  protected final String localPartitionRoot;
  protected final String localPartitionRootCache;
  private final IncrementalUpdatePlanner updatePlanner;
  private Coordinator coordinatorToCloseOpportunistically;

  public IncrementalPartitionUpdater(Domain domain,
                                     String localPartitionRoot,
                                     IncrementalUpdatePlanner updatePlanner) throws IOException {
    this.domain = domain;
    this.localPartitionRoot = localPartitionRoot;
    this.localPartitionRootCache = localPartitionRoot + "/" + CACHE_ROOT_NAME;
    this.updatePlanner = updatePlanner;
  }

  /**
   * @return The current valid version number or null if there is none
   * @throws IOException
   */
  protected abstract Integer detectCurrentVersionNumber() throws IOException;

  protected abstract Set<DomainVersion> detectCachedBasesCore() throws IOException;

  protected abstract Set<DomainVersion> detectCachedDeltasCore() throws IOException;

  protected abstract void cleanCachedVersions() throws IOException;

  protected abstract void fetchVersion(DomainVersion version, String fetchRoot) throws IOException;

  protected abstract void runUpdateCore(DomainVersion currentVersion,
                                        DomainVersion updatingToVersion,
                                        IncrementalUpdatePlan updatePlan,
                                        String updateWorkRoot,
                                        PartitionUpdateTaskStatistics statistics) throws IOException;

  @Override
  public void updateTo(DomainVersion updatingToVersion, PartitionUpdateTaskStatistics statistics) throws IOException {
    ensureLocalPartitionRootExists();
    ensureCacheExists();
    try {
      DomainVersion currentVersion = detectCurrentVersion();
      Set<DomainVersion> cachedBases = detectCachedBases();
      Set<DomainVersion> cachedDeltas = detectCachedDeltas();
      IncrementalUpdatePlan updatePlan = updatePlanner.computeUpdatePlan(currentVersion, cachedBases, updatingToVersion);
      // The plan is empty, we are done
      if (updatePlan == null) {
        return;
      }
      LOG.info("Using update plan " + updatePlan + " to update " + localPartitionRoot);
      // At this point, we can close the Coordinator opportunistically if requested
      closeCoordinatorOpportunistically();
      // Fetch and cache versions needed to update
      HankTimer timer = new HankTimer();
      cacheVersionsNeededToUpdate(currentVersion, cachedBases, cachedDeltas, updatePlan);
      long fetchTimeMs = timer.getDurationMs();
      statistics.getDurationsMs().put("Update data fetch", fetchTimeMs);
      // Run update in a workspace
      timer.restart();
      runUpdate(currentVersion, updatingToVersion, updatePlan, statistics);
      long executionTimeMs = timer.getDurationMs();
      statistics.getDurationsMs().put("Update execution", executionTimeMs);
      LOG.info("Update in " + localPartitionRoot + " to " + updatingToVersion
          + ": fetched data in " + FormatUtils.formatSecondsDuration(fetchTimeMs / 1000)
          + ", executed in " + FormatUtils.formatSecondsDuration(executionTimeMs / 1000));
    } finally {
      cleanCachedVersions();
    }
  }

  private void closeCoordinatorOpportunistically() throws IOException {
    if (coordinatorToCloseOpportunistically != null) {
      coordinatorToCloseOpportunistically.close();
    }
  }

  public Set<DomainVersion> detectCachedBases() throws IOException {
    return detectCachedBasesCore();
  }

  public Set<DomainVersion> detectCachedDeltas() throws IOException {
    return detectCachedDeltasCore();
  }

  // Fetch required versions and commit them to cache upon successful fetch
  protected void cacheVersionsNeededToUpdate(DomainVersion currentVersion,
                                             Set<DomainVersion> cachedBases,
                                             Set<DomainVersion> cachedDeltas,
                                             IncrementalUpdatePlan updatePlan) throws IOException {
    try {
      ensureCacheExists();
      // Clean all previous fetch roots
      deleteFetchRoots();
      // Create new fetch root
      File fetchRoot = createFetchRoot();
      // Fetch versions
      for (DomainVersion version : updatePlan.getAllVersions()) {
        // Do not fetch current version
        if (currentVersion != null && currentVersion.equals(version)) {
          continue;
        }
        // Do not fetch cached versions
        if (cachedBases.contains(version) || cachedDeltas.contains(version)) {
          continue;
        }
        fetchVersion(version, fetchRoot.getAbsolutePath());
      }
      // Commit fetched versions to cache
      commitFiles(fetchRoot, localPartitionRootCache);
    } finally {
      // Always delete fetch roots
      deleteFetchRoots();
    }
  }

  private void runUpdate(DomainVersion currentVersion,
                         DomainVersion updatingToVersion,
                         IncrementalUpdatePlan updatePlan,
                         PartitionUpdateTaskStatistics statistics) throws IOException {
    // Clean all previous update work roots
    deleteUpdateWorkRoots();
    // Create new update work root
    File updateWorkRoot = createUpdateWorkRoot();
    try {
      // Execute update
      runUpdateCore(currentVersion, updatingToVersion, updatePlan, updateWorkRoot.getAbsolutePath(), statistics);
      // Move current version to cache
      commitFiles(new File(localPartitionRoot), localPartitionRootCache);
      // Commit update result files to top level
      commitFiles(updateWorkRoot, localPartitionRoot);
    } finally {
      deleteUpdateWorkRoots();
    }
  }

  // Move all files in sourceRoot to destinationRoot. Directories are ignored.
  protected void commitFiles(File sourceRoot, String destinationRoot) throws IOException {
    File[] files = sourceRoot.listFiles();
    if (files == null) {
      throw new IOException("Failed to commit files from " + sourceRoot + " to " + destinationRoot + " since source is not a valid directory.");
    }
    for (File file : files) {
      // Skip non files
      if (!file.isFile()) {
        continue;
      }
      File targetFile = new File(destinationRoot + "/" + file.getName());
      // If target file already exists, delete it
      if (targetFile.exists()) {
        if (!targetFile.delete()) {
          throw new IOException("Failed to overwrite file in destination root: " + targetFile.getAbsolutePath());
        }
      }
      // Move file to destination
      if (!file.renameTo(targetFile)) {
        LOG.info("Committing " + file.getAbsolutePath() + " to " + targetFile.getAbsolutePath());
        throw new IOException("Failed to rename source file: " + file.getAbsolutePath()
            + " to destination file: " + targetFile.getAbsolutePath());
      }
    }
  }

  public void ensureLocalPartitionRootExists() throws IOException {
    // Create cache directory if it doesn't exist
    File rootFile = new File(localPartitionRoot);
    if (!rootFile.exists()) {
      if (!rootFile.mkdirs()) {
        throw new IOException("Failed to create local partition root directory: " + rootFile.getAbsolutePath());
      }
    }
  }

  public void ensureCacheExists() throws IOException {
    // Create cache directory if it doesn't exist
    File cacheRootFile = new File(localPartitionRootCache);
    if (!cacheRootFile.exists()) {
      if (!cacheRootFile.mkdir()) {
        throw new IOException("Failed to create cache root directory: " + cacheRootFile.getAbsolutePath());
      }
    }
  }

  private DomainVersion detectCurrentVersion() throws IOException {
    Integer currentVersionNumber = detectCurrentVersionNumber();
    if (currentVersionNumber != null) {
      return domain.getVersion(currentVersionNumber);
    } else {
      return null;
    }
  }

  private File createUpdateWorkRoot() throws IOException {
    return createTmpWorkRoot(UPDATE_WORK_ROOT_PREFIX);
  }

  private void deleteUpdateWorkRoots() throws IOException {
    deleteTmpWorkRoots(UPDATE_WORK_ROOT_PREFIX);
  }

  private File createFetchRoot() throws IOException {
    return createTmpWorkRoot(FETCH_ROOT_PREFIX);
  }

  private void deleteFetchRoots() throws IOException {
    deleteTmpWorkRoots(FETCH_ROOT_PREFIX);
  }

  private File createTmpWorkRoot(String prefix) throws IOException {
    String tmpRoot = localPartitionRoot + "/" + prefix + UUID.randomUUID().toString();
    File tmpRootFile = new File(tmpRoot);
    if (!tmpRootFile.mkdir()) {
      throw new IOException("Failed to create temporary work root: " + tmpRoot);
    }
    return tmpRootFile;
  }

  private void deleteTmpWorkRoots(String prefix) throws IOException {
    for (File file : new File(localPartitionRoot).listFiles()) {
      if (file.isDirectory() && file.getName().startsWith(prefix)) {
        FileUtils.deleteDirectory(file);
      }
    }
  }

  @Override
  public void closeCoordinatorOpportunistically(Coordinator coordinator) {
    this.coordinatorToCloseOpportunistically = coordinator;
  }
}
