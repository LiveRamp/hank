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
import com.rapleaf.hank.storage.IncrementalUpdatePlan;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.cueball.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class CurlyCompactingPartitionUpdater extends AbstractCurlyPartitionUpdater {

  private final ICurlyCompactingMerger merger;
  private final ICueballStreamBufferMergeSortFactory cueballStreamBufferMergeSortFactory;
  private final ICurlyWriterFactory curlyWriterFactory;

  public CurlyCompactingPartitionUpdater(Domain domain,
                                         PartitionRemoteFileOps partitionRemoteFileOps,
                                         String localPartitionRoot,
                                         ICurlyCompactingMerger merger,
                                         ICueballStreamBufferMergeSortFactory cueballStreamBufferMergeSortFactory,
                                         ICurlyWriterFactory curlyWriterFactory) throws IOException {
    super(domain, partitionRemoteFileOps, localPartitionRoot);
    this.merger = merger;
    this.cueballStreamBufferMergeSortFactory = cueballStreamBufferMergeSortFactory;
    this.curlyWriterFactory = curlyWriterFactory;
  }

  @Override
  protected void runUpdateCore(DomainVersion currentVersion,
                               DomainVersion updatingToVersion,
                               IncrementalUpdatePlan updatePlan,
                               String updateWorkRoot) throws IOException {


    // Prepare Curly

    // Determine files from versions
    CurlyFilePath curlyBasePath = getCurlyFilePathForVersion(updatePlan.getBase(), currentVersion, true);
    List<CurlyFilePath> curlyDeltas = new ArrayList<CurlyFilePath>();
    for (DomainVersion curlyDeltaVersion : updatePlan.getDeltasOrdered()) {
      // Only add to the delta list if the version is not empty
      if (!isEmptyVersion(curlyDeltaVersion)) {
        curlyDeltas.add(getCurlyFilePathForVersion(curlyDeltaVersion, currentVersion, false));
      }
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
      if (!CueballPartitionUpdater.isEmptyVersion(cueballDelta, partitionRemoteFileOps)) {
        cueballDeltas.add(CueballPartitionUpdater.getCueballFilePathForVersion(cueballDelta, currentVersion,
            localPartitionRoot, localPartitionRootCache, false));
      }
    }

    // Check that all required files are available
    CueballPartitionUpdater.checkRequiredFileExists(cueballBasePath.getPath());
    for (CueballFilePath cueballDelta : cueballDeltas) {
      CueballPartitionUpdater.checkRequiredFileExists(cueballDelta.getPath());
    }

    // Determine new Curly base path
    CurlyFilePath newCurlyBasePath =
        new CurlyFilePath(updateWorkRoot + "/" + Curly.getName(updatingToVersion.getVersionNumber(), true));

    // Determine new Cueball base path
    CueballFilePath newCueballBasePath =
        new CueballFilePath(updateWorkRoot + "/" + Cueball.getName(updatingToVersion.getVersionNumber(), true));

    OutputStream newCurlyBaseOutputStream = new FileOutputStream(newCurlyBasePath.getPath());
    OutputStream newCueballBaseOutputStream = new FileOutputStream(newCueballBasePath.getPath());

    // Note: the Curly writer used to perform the compaction must not hash the passed-in key because
    // it will directly receive key hashes. This is because the actual key is unknown when compacting.
    CurlyWriter curlyWriter = curlyWriterFactory.getCurlyWriter(newCueballBaseOutputStream, newCurlyBaseOutputStream);

    IKeyFileStreamBufferMergeSort cueballStreamBufferMergeSort =
        cueballStreamBufferMergeSortFactory.getInstance(cueballBasePath, cueballDeltas);

    merger.merge(curlyBasePath, curlyDeltas, cueballStreamBufferMergeSort, curlyWriter);
  }
}
