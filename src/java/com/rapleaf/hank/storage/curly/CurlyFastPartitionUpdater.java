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
import com.rapleaf.hank.storage.IncrementalUpdatePlan;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.cueball.CueballPartitionUpdater;
import com.rapleaf.hank.storage.cueball.ICueballMerger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CurlyFastPartitionUpdater extends AbstractCurlyPartitionUpdater {

  private final int keyHashSize;
  private final int offsetSize;
  private final int hashIndexBits;
  private final CompressionCodec compressionCodec;
  private final ICurlyMerger curlyMerger;
  private final ICueballMerger cueballMerger;

  public CurlyFastPartitionUpdater(Domain domain,
                                   PartitionRemoteFileOps partitionRemoteFileOps,
                                   ICurlyMerger curlyMerger,
                                   ICueballMerger cueballMerger,
                                   int keyHashSize,
                                   int offsetSize,
                                   int hashIndexBits,
                                   CompressionCodec compressionCodec,
                                   String localPartitionRoot) throws IOException {
    super(domain,
        partitionRemoteFileOps,
        localPartitionRoot);
    this.keyHashSize = keyHashSize;
    this.offsetSize = offsetSize;
    this.hashIndexBits = hashIndexBits;
    this.compressionCodec = compressionCodec;
    this.curlyMerger = curlyMerger;
    this.cueballMerger = cueballMerger;
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

    // Determine files from versions
    CurlyFilePath curlyBase = getCurlyFilePathForVersion(updatePlan.getBase(), currentVersion, true);
    List<CurlyFilePath> curlyDeltas = new ArrayList<CurlyFilePath>();
    for (DomainVersion curlyDeltaVersion : updatePlan.getDeltasOrdered()) {
      // Only add to the delta list if the version is not empty
      if (!isEmptyVersion(partitionRemoteFileOps, curlyDeltaVersion)) {
        curlyDeltas.add(getCurlyFilePathForVersion(curlyDeltaVersion, currentVersion, false));
      }
    }

    // Check that all required files are available
    CueballPartitionUpdater.checkRequiredFileExists(curlyBase.getPath());
    for (CurlyFilePath curlyDelta : curlyDeltas) {
      CueballPartitionUpdater.checkRequiredFileExists(curlyDelta.getPath());
    }

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

    // Merge the Curly files
    long[] offsetAdjustments = curlyMerger.merge(newCurlyBasePath, curlyDeltas);

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
        offsetSize,
        hashIndexBits,
        compressionCodec,
        new OffsetTransformer(offsetSize, offsetAdjustments));
  }
}
