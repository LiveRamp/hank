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
import com.rapleaf.hank.storage.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.SortedSet;

public class CueballUpdater extends AbstractLocalFetcherUpdater {
  private static final Logger LOG = Logger.getLogger(CueballUpdater.class);

  private final int keyHashSize;
  private final int valueSize;
  private final ICueballMerger merger;
  private final CompressionCodec compressionCodec;
  private final int hashIndexBits;

  CueballUpdater(String localPartitionRoot,
                 int keyHashSize,
                 int valueSize,
                 IFetcher fetcher,
                 ICueballMerger merger,
                 CompressionCodec compressionCodec,
                 int hashIndexBits) {
    super(fetcher, localPartitionRoot);
    this.keyHashSize = keyHashSize;
    this.valueSize = valueSize;
    this.merger = merger;
    this.compressionCodec = compressionCodec;
    this.hashIndexBits = hashIndexBits;
  }

  public CueballUpdater(String localPartitionRoot,
                        int keyHashSize,
                        int valueSize,
                        IFileOps fileOps,
                        IFileSelector fileSelector,
                        CompressionCodec compressionCodec,
                        int hashIndexBits) {
    this(localPartitionRoot,
        keyHashSize,
        valueSize,
        new Fetcher(fileOps, fileSelector),
        new CueballMerger(),
        compressionCodec,
        hashIndexBits);
  }

  protected void runUpdate(int toVersion) throws IOException {
    SortedSet<CueballFilePath> bases = Cueball.getBases(getLocalRoot(), getLocalWorkspaceRoot());
    SortedSet<CueballFilePath> deltas = Cueball.getDeltas(getLocalRoot(), getLocalWorkspaceRoot());

    // merge the latest base and all the deltas newer than it
    if (bases.isEmpty()) {
      LOG.info("Didn't find any bases. Using first delta instead.");
      if (deltas.isEmpty()) {
        throw new IllegalStateException("There are no bases or deltas in "
            + getLocalWorkspaceRoot() + " after the fetcher ran!");
      }
      bases.add(deltas.first());
      deltas.remove(deltas.first());
    }
    CueballFilePath latestBase = bases.last();
    SortedSet<CueballFilePath> relevantDeltas = deltas.tailSet(latestBase);

    String newBasePath = getLocalWorkspaceRoot() + "/"
        + Cueball.padVersionNumber(toVersion)
        + ".base.cueball";

    merger.merge(latestBase,
        relevantDeltas,
        newBasePath,
        keyHashSize,
        valueSize,
        null,
        hashIndexBits,
        compressionCodec);

    // delete all the old bases
    // TODO: pay attention to this. it could fail and should be logged in that
    // situation
    deleteCueballFiles(bases);
    // delete all deltas
    // TODO: pay attention to this. it could fail and should be logged in that
    // situation
    deleteCueballFiles(deltas);
  }

  protected int getLatestLocalVersionNumber() {
    SortedSet<CueballFilePath> bases = Cueball.getBases(getLocalRoot());
    if (bases != null && bases.size() > 0) {
      return bases.last().getVersion();
    } else {
      return -1;
    }
  }

  public static void deleteCueballFiles(SortedSet<CueballFilePath> file) throws IOException {
    for (PartitionFileLocalPath p : file) {
      PartitionFileLocalPath.delete(p);
    }
  }
}
