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

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.cueball.CueballPartitionUpdater;
import com.rapleaf.hank.storage.cueball.ICueballMerger;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.hank.storage.incremental.IncrementalUpdatePlan;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CurlyFastPartitionUpdater extends AbstractCurlyPartitionUpdater {

  private static final Logger LOG = Logger.getLogger(CurlyFastPartitionUpdater.class);

  private final int keyHashSize;
  private final int offsetNumBytes;
  private final int hashIndexBits;
  private final CompressionCodec compressionCodec;
  private final ICurlyMerger curlyMerger;
  private final ICueballMerger cueballMerger;

  public CurlyFastPartitionUpdater(Domain domain,
                                   PartitionRemoteFileOps partitionRemoteFileOps,
                                   ICurlyMerger curlyMerger,
                                   ICueballMerger cueballMerger,
                                   int keyHashSize,
                                   int offsetNumBytes,
                                   int hashIndexBits,
                                   CompressionCodec compressionCodec,
                                   String localPartitionRoot) throws IOException {
    super(domain,
        partitionRemoteFileOps,
        localPartitionRoot);
    this.keyHashSize = keyHashSize;
    this.offsetNumBytes = offsetNumBytes;
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
                               String updateWorkRoot) throws IOException {
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
    long[] offsetAdjustments = curlyMerger.merge(newCurlyBasePath, curlyDeltaRemoteFiles, partitionRemoteFileOps);

    // Run Cueball update
    CueballPartitionUpdater.runUpdateCore(partitionRemoteFileOps,
        currentVersion,
        updatingToVersion,
        updatePlan,
        updateWorkRoot,
        localPartitionRoot,
        localPartitionRootCache,
        cueballMerger,
        keyHashSize,
        offsetNumBytes,
        hashIndexBits,
        compressionCodec,
        new OffsetTransformer(offsetNumBytes, offsetAdjustments));
  }
}
