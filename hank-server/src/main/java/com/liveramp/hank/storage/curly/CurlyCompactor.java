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

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.partition_server.PartitionUpdateTaskStatistics;
import com.liveramp.hank.storage.Compactor;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.Writer;
import com.liveramp.hank.storage.cueball.CueballFilePath;
import com.liveramp.hank.storage.cueball.CueballPartitionUpdater;
import com.liveramp.hank.storage.cueball.ICueballStreamBufferMergeSortFactory;
import com.liveramp.hank.storage.cueball.IKeyFileStreamBufferMergeSort;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlan;
import com.liveramp.hank.util.HankTimer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CurlyCompactor extends AbstractCurlyPartitionUpdater implements Compactor {

  private final ICurlyCompactingMerger merger;
  private final ICueballStreamBufferMergeSortFactory cueballStreamBufferMergeSortFactory;
  private final ICurlyReaderFactory curlyReaderFactory;
  private Writer writer;

  public CurlyCompactor(Domain domain,
                        PartitionRemoteFileOps partitionRemoteFileOps,
                        String localPartitionRoot,
                        ICurlyCompactingMerger merger,
                        ICueballStreamBufferMergeSortFactory cueballStreamBufferMergeSortFactory,
                        ICurlyReaderFactory curlyReaderFactory) throws IOException {
    super(domain, partitionRemoteFileOps, localPartitionRoot);
    this.merger = merger;
    this.cueballStreamBufferMergeSortFactory = cueballStreamBufferMergeSortFactory;
    this.curlyReaderFactory = curlyReaderFactory;
  }

  @Override
  public void compact(DomainVersion versionToCompact,
                      Writer writer) throws IOException {
    this.writer = writer;
    this.updateTo(versionToCompact, new PartitionUpdateTaskStatistics());
  }

  @Override
  protected boolean shouldFetchCurlyVersion(DomainVersion version) throws IOException {
    // The CurlyCompactor needs all versions of curly
    return true;
  }

  @Override
  protected void runUpdateCore(DomainVersion currentVersion,
                               DomainVersion updatingToVersion,
                               IncrementalUpdatePlan updatePlan,
                               String updateWorkRoot,
                               PartitionUpdateTaskStatistics statistics) throws IOException {

    // Prepare Curly

    // Determine files from versions
    CurlyFilePath curlyBasePath = getCurlyFilePathForVersion(updatePlan.getBase(), currentVersion, true);
    List<CurlyFilePath> curlyDeltas = new ArrayList<CurlyFilePath>();
    for (DomainVersion curlyDeltaVersion : updatePlan.getDeltasOrdered()) {
      curlyDeltas.add(getCurlyFilePathForVersion(curlyDeltaVersion, currentVersion, false));
    }

    // Check that all required files are available
    CueballPartitionUpdater.checkRequiredFileExists(curlyBasePath.getPath());
    for (CurlyFilePath curlyDelta : curlyDeltas) {
      CueballPartitionUpdater.checkRequiredFileExists(curlyDelta.getPath());
    }

    // Prepare Cueball

    // Determine files from versions
    CueballFilePath cueballBasePath = CueballPartitionUpdater.getCueballFilePathForVersion(updatePlan.getBase(),
        currentVersion, localPartitionRoot, localPartitionRootCache, true);
    List<CueballFilePath> cueballDeltas = new ArrayList<CueballFilePath>();
    for (DomainVersion cueballDelta : updatePlan.getDeltasOrdered()) {
      // Only add to the delta list if the version is not empty
      if (!CueballPartitionUpdater.isEmptyVersion(partitionRemoteFileOps, cueballDelta)) {
        cueballDeltas.add(CueballPartitionUpdater.getCueballFilePathForVersion(cueballDelta, currentVersion,
            localPartitionRoot, localPartitionRootCache, false));
      }
    }

    // Check that all required files are available
    CueballPartitionUpdater.checkRequiredFileExists(cueballBasePath.getPath());
    for (CueballFilePath cueballDelta : cueballDeltas) {
      CueballPartitionUpdater.checkRequiredFileExists(cueballDelta.getPath());
    }

    // Note: the writer used to perform the compaction must not hash the passed-in key because
    // it will directly receive key hashes. This is because the actual key is unknown when compacting.
    IKeyFileStreamBufferMergeSort cueballStreamBufferMergeSort =
        cueballStreamBufferMergeSortFactory.getInstance(cueballBasePath, cueballDeltas);

    HankTimer timer = new HankTimer();
    merger.merge(curlyBasePath, curlyDeltas, cueballStreamBufferMergeSort, curlyReaderFactory, writer);
    statistics.getDurationsMs().put("Curly compaction", timer.getDurationMs());
  }
}
