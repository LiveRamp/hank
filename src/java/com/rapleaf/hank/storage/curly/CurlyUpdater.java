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
import com.rapleaf.hank.storage.*;
import com.rapleaf.hank.storage.cueball.*;
import com.rapleaf.hank.util.EncodingHelper;

import java.io.File;
import java.io.IOException;
import java.util.SortedSet;

import static com.rapleaf.hank.storage.cueball.CueballUpdater.deleteCueballFiles;

public class CurlyUpdater extends AbstractLocalFetcherUpdater {

  public static final class OffsetTransformer implements ValueTransformer {
    private final long[] offsetAdjustments;
    private final int offsetSize;

    public OffsetTransformer(int offsetSize, long[] offsetAdjustments) {
      this.offsetSize = offsetSize;
      this.offsetAdjustments = offsetAdjustments;
    }

    @Override
    public void transform(byte[] buf, int valueOff, int relIndex) {
      long adjustment = offsetAdjustments[relIndex];
      if (adjustment != 0) {
        long offset = EncodingHelper.decodeLittleEndianFixedWidthLong(buf, valueOff, offsetSize);
        offset += adjustment;
        EncodingHelper.encodeLittleEndianFixedWidthLong(offset, buf, valueOff, offsetSize);
      }
    }
  }

  private final int keyHashSize;
  private final int offsetSize;
  private final ICurlyMerger curlyMerger;
  private final ICueballMerger cueballMerger;
  private final int hashIndexBits;
  private final CompressionCodec compressionCodec;

  public CurlyUpdater(String localPartitionRoot, String remotePartitionRoot, int keyHashSize, int offsetSize, IFileOps fileOps, CompressionCodec compressionCodec, int hashIndexBits) {
    this(localPartitionRoot,
        keyHashSize,
        offsetSize,
        new Fetcher(fileOps, new CurlyFileSelector()),
        new CurlyMerger(),
        new CueballMerger(),
        compressionCodec,
        hashIndexBits);
  }

  CurlyUpdater(String localPartitionRoot,
               int keyHashSize,
               int offsetSize,
               IFetcher fetcher,
               ICurlyMerger curlyMerger,
               ICueballMerger cueballMerger,
               CompressionCodec compressonCodec,
               int hashIndexBits) {
    super(fetcher, localPartitionRoot);
    this.keyHashSize = keyHashSize;
    this.offsetSize = offsetSize;
    this.curlyMerger = curlyMerger;
    this.cueballMerger = cueballMerger;
    this.compressionCodec = compressonCodec;
    this.hashIndexBits = hashIndexBits;
  }

  protected void runUpdate(int toVersion) throws IOException {
    // figure out which curly files we want to merge
    SortedSet<CurlyFilePath> curlyBases = Curly.getBases(getLocalRoot(), getLocalWorkspaceRoot());
    SortedSet<CurlyFilePath> curlyDeltas = Curly.getDeltas(getLocalRoot(), getLocalWorkspaceRoot());

    // merge the latest base and all the deltas newer than it
    if (curlyBases.isEmpty()) {
      if (curlyDeltas.isEmpty()) {
        throw new IllegalStateException("There are no curly bases in "
            + getLocalRoot() + " and " + getLocalWorkspaceRoot() + " after the fetcher ran!");
      }
      curlyBases.add(curlyDeltas.first());
      curlyDeltas.remove(curlyDeltas.first());
    }

    // Move current latest base to workspace dir and immediately name it the correct final name
    CurlyFilePath currentLatestCurlyBase = curlyBases.last();
    CurlyFilePath newCurlyBasePath = new CurlyFilePath(getLocalWorkspaceRoot()
        + "/" + Curly.padVersionNumber(toVersion) + ".base.curly");
    if (!new File(currentLatestCurlyBase.getPath()).renameTo(new File(newCurlyBasePath.getPath()))) {
      throw new IOException("Failed to rename Curly base " + currentLatestCurlyBase.getPath()
          + " to " + newCurlyBasePath.getPath());
    }

    SortedSet<CurlyFilePath> relevantCurlyDeltas = curlyDeltas.tailSet(currentLatestCurlyBase);

    // merge the curly files
    long[] offsetAdjustments = curlyMerger.merge(newCurlyBasePath, relevantCurlyDeltas);

    // figure out which cueball files we want to merge
    SortedSet<CueballFilePath> cueballBases = Cueball.getBases(getLocalRoot(), getLocalWorkspaceRoot());
    SortedSet<CueballFilePath> cueballDeltas = Cueball.getDeltas(getLocalRoot(), getLocalWorkspaceRoot());
    if (cueballBases.isEmpty()) {
      if (cueballDeltas.isEmpty()) {
        throw new IllegalStateException("There are no cueball bases or deltas in "
            + getLocalRoot() + " and " + getLocalWorkspaceRoot() + " after the fetcher ran!");
      }
      cueballBases.add(cueballDeltas.first());
      cueballDeltas.remove(cueballDeltas.first());
    }
    CueballFilePath latestCueballBase = cueballBases.last();
    SortedSet<CueballFilePath> relevantCueballDeltas = cueballDeltas.tailSet(latestCueballBase);

    // Determine new cueball base path
    String newCueballBasePath = getLocalWorkspaceRoot() + "/" + Cueball.padVersionNumber(toVersion) + ".base.cueball";

    // If no deltas, simply rename the base
    if (relevantCueballDeltas.isEmpty()) {
      if (!new File(latestCueballBase.getPath()).renameTo(new File(newCueballBasePath))) {
        throw new IOException("Failed to rename Cueball base from " + latestCueballBase.getPath()
            + " to " + newCueballBasePath);
      }
    } else {
      // Build new cueball base
      cueballMerger.merge(latestCueballBase,
          relevantCueballDeltas,
          newCueballBasePath,
          keyHashSize,
          offsetSize,
          new OffsetTransformer(offsetSize, offsetAdjustments),
          hashIndexBits,
          compressionCodec);
    }

    // Delete all the old files
    // TODO: this can fail. watch it.
    // Skip currentLatestCurlyBase because it has been renamed
    deleteCurlyFiles(curlyBases.headSet(currentLatestCurlyBase));
    // If there was no Cueball deltas, we simply renamed the base, so don't try to delete it
    if (relevantCueballDeltas.isEmpty()) {
      deleteCueballFiles(cueballBases.headSet(latestCueballBase));
    } else {
      deleteCueballFiles(cueballBases);
    }
    deleteCurlyFiles(curlyDeltas);
    deleteCueballFiles(cueballDeltas);
  }

  protected int getLatestLocalVersionNumber() {
    SortedSet<CurlyFilePath> bases = Curly.getBases(getLocalRoot());
    if (bases != null && bases.size() > 0) {
      return bases.last().getVersion();
    } else {
      return -1;
    }
  }

  public static void deleteCurlyFiles(SortedSet<CurlyFilePath> file) throws IOException {
    for (PartitionFileLocalPath p : file) {
      PartitionFileLocalPath.delete(p);
    }
  }
}
