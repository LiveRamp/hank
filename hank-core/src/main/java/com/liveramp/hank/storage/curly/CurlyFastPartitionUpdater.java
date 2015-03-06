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

package com.liveramp.hank.storage.curly;

import com.liveramp.hank.compression.cueball.CueballCompressionCodec;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.partition_server.PartitionUpdateTaskStatistics;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.cueball.CueballPartitionUpdater;
import com.liveramp.hank.storage.cueball.ICueballMerger;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlan;
import com.liveramp.hank.util.FormatUtils;
import com.liveramp.hank.util.HankTimer;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CurlyFastPartitionUpdater extends AbstractCurlyPartitionUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(CurlyFastPartitionUpdater.class);

  private final int keyHashSize;
  private final int offsetNumBytes;
  private final int valueSize;
  private final int hashIndexBits;
  private final CueballCompressionCodec compressionCodec;
  private final ICurlyMerger curlyMerger;
  private final ICueballMerger cueballMerger;

  public CurlyFastPartitionUpdater(Domain domain,
                                   PartitionRemoteFileOps partitionRemoteFileOps,
                                   ICurlyMerger curlyMerger,
                                   ICueballMerger cueballMerger,
                                   int keyHashSize,
                                   int offsetNumBytes,
                                   int offsetInBlockNumBytes,
                                   int hashIndexBits,
                                   CueballCompressionCodec compressionCodec,
                                   String localPartitionRoot) throws IOException {
    super(domain, partitionRemoteFileOps, localPartitionRoot);
    this.keyHashSize = keyHashSize;
    this.offsetNumBytes = offsetNumBytes;
    if (offsetInBlockNumBytes > 0) {
      this.valueSize = offsetNumBytes + offsetInBlockNumBytes;
    } else {
      this.valueSize = offsetNumBytes;
    }
    this.hashIndexBits = hashIndexBits;
    this.compressionCodec = compressionCodec;
    this.curlyMerger = curlyMerger;
    this.cueballMerger = cueballMerger;
  }

  @Override
  protected boolean shouldFetchCurlyVersion(DomainVersion version) throws IOException {
    // If this is the base, we also need to fetch the Curly file. Otherwise,
    // the Curly fetching is done on the fly during the update.
    return IncrementalDomainVersionProperties.isBase(version);
  }

  @Override
  protected void runUpdateCore(DomainVersion currentVersion,
                               DomainVersion updatingToVersion,
                               IncrementalUpdatePlan updatePlan,
                               String updateWorkRoot,
                               PartitionUpdateTaskStatistics statistics) throws IOException {
    // Run Curly update

    // Determine new base path
    CurlyFilePath newCurlyBasePath =
        new CurlyFilePath(updateWorkRoot + "/" + Curly.getName(updatingToVersion.getVersionNumber(), true));

    // Determine base file from version
    CurlyFilePath curlyBase = getCurlyFilePathForVersion(updatePlan.getBase(), currentVersion, true);

    // Check that base file is available
    CueballPartitionUpdater.checkRequiredFileExists(curlyBase.getPath());

    // Move the Curly base to the final destination, overwriting it
    File newCurlyBaseFile = new File(newCurlyBasePath.getPath());
    if (newCurlyBaseFile.exists()) {
      if (!newCurlyBaseFile.delete()) {
        throw new IOException("Failed to overwrite Curly base " + newCurlyBaseFile.getAbsolutePath());
      }
    }
    if (!new File(curlyBase.getPath()).renameTo(newCurlyBaseFile)) {
      throw new IOException("Failed to move Curly base " + curlyBase.getPath() + " to " + newCurlyBasePath);
    }

    // Determine delta files from versions
    List<String> curlyDeltaRemoteFiles = new ArrayList<String>();
    for (DomainVersion curlyDeltaVersion : updatePlan.getDeltasOrdered()) {
      curlyDeltaRemoteFiles.add(Curly.getName(curlyDeltaVersion));
    }

    // Merge the Curly delta files into the base
    HankTimer timer = new HankTimer();
    long[] offsetAdjustments = curlyMerger.merge(newCurlyBasePath, curlyDeltaRemoteFiles, partitionRemoteFileOps);
    long curlyTimeMs = timer.getDurationMs();

    // Run Cueball update
    timer.restart();
    CueballPartitionUpdater.runUpdateCore(
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
        new OffsetTransformer(offsetNumBytes, offsetAdjustments),
        statistics);
    long cueballTimeMs = timer.getDurationMs();

    statistics.getDurationsMs().put("Curly merge", curlyTimeMs);

    LOG.info("Update in " + updateWorkRoot + " to " + updatingToVersion
        + ": merged Curly deltas in " + FormatUtils.formatSecondsDuration(curlyTimeMs / 1000)
        + ", ran Cueball update in " + FormatUtils.formatSecondsDuration(cueballTimeMs / 1000));
  }
}
