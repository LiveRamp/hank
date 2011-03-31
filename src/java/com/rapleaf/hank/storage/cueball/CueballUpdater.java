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

import java.io.File;
import java.io.IOException;
import java.util.SortedSet;

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.storage.Updater;

public class CueballUpdater implements Updater {

  private final String localPartitionRoot;
  private final int keyHashSize;
  private final int valueSize;
  private final IFetcher fetcher;
  private final ICueballMerger merger;
  private final CompressionCodec compressionCodec;
  private final int hashIndexBits;

  CueballUpdater(String localPartitionRoot,
      int keyHashSize,
      int valueSize,
      IFetcher fetcher,
      ICueballMerger merger,
      CompressionCodec compressionCodec,
      int hashIndexBits)
  {
    this.localPartitionRoot = localPartitionRoot;
    this.keyHashSize = keyHashSize;
    this.valueSize = valueSize;
    this.fetcher = fetcher;
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
      int hashIndexBits)
  {
    this(localPartitionRoot,
        keyHashSize,
        valueSize,
        new Fetcher(fileOps, fileSelector),
        new CueballMerger(),
        compressionCodec,
        hashIndexBits);
  }

  @Override
  public void update(int toVersion) throws IOException {
    fetcher.fetch(getLocalVersionNumber(), toVersion);
    resolveLocalDir();
  }

  private void resolveLocalDir() throws IOException {
    SortedSet<String> bases = Cueball.getBases(localPartitionRoot);
    SortedSet<String> deltas = Cueball.getDeltas(localPartitionRoot);

    // merge the latest base and all the deltas newer than it
    if (bases.isEmpty()) {
      throw new IllegalStateException("There are no bases in "
          + localPartitionRoot + " after the fetcher ran!");
    }
    String latestBase = bases.last();
    SortedSet<String> relevantDeltas = deltas.tailSet(latestBase);

    String newBasePath = localPartitionRoot + "/"
        + Cueball.padVersion(Cueball.parseVersionNumber(relevantDeltas.last()))
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
    for (String oldBase : bases) {
      // TODO: pay attention to this. it could fail and should be logged in that
      // situation
      new File(oldBase).delete();
    }

    // delete all deltas
    for (String oldDelta : deltas) {
      // TODO: pay attention to this. it could fail and should be logged in that
      // situation
      new File(oldDelta).delete();
    }
  }

  private int getLocalVersionNumber() {
    File local = new File(localPartitionRoot);
    String[] filesInLocal = local.list();

    int bestVer = -1;

    // identify all the bases and deltas
    for (String file : filesInLocal) {
      if (file.matches(Cueball.BASE_REGEX)) {
        int thisVer = Cueball.parseVersionNumber(file);
        if (thisVer > bestVer) {
          bestVer = thisVer;
        }
      }
    }
    return bestVer;
  }
}
