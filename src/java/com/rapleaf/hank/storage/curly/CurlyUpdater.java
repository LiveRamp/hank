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

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.cueball.Cueball;
import com.rapleaf.hank.storage.cueball.CueballMerger;
import com.rapleaf.hank.storage.cueball.Fetcher2;
import com.rapleaf.hank.storage.cueball.ICueballMerger;
import com.rapleaf.hank.storage.cueball.IFetcher;
import com.rapleaf.hank.storage.cueball.IFileOps;
import com.rapleaf.hank.storage.cueball.StreamBuffer;
import com.rapleaf.hank.storage.cueball.ValueTransformer;
import com.rapleaf.hank.util.EncodingHelper;

public class CurlyUpdater implements Updater {
  public static final class OffsetTransformer implements ValueTransformer {
    private final long[] offsetAdjustments;
    private final int keyHashSize;
    private final int offsetSize;

    public OffsetTransformer(int keyHashSize, int offsetSize, long[] offsetAdjustments) {
      this.keyHashSize = keyHashSize;
      this.offsetSize = offsetSize;
      this.offsetAdjustments = offsetAdjustments;
    }

    @Override
    public void transform(StreamBuffer buffer) {
      long adjustment = offsetAdjustments[buffer.getIndex()];
      if (adjustment != 0) {
        long offset = EncodingHelper.decodeLittleEndianFixedWidthLong(buffer.getBuffer(), buffer.getCurrentOffset() + keyHashSize, offsetSize);
        offset += adjustment;
        EncodingHelper.encodeLittleEndianFixedWidthLong(offset, buffer.getBuffer(), buffer.getCurrentOffset() + keyHashSize, offsetSize);
      }
    }
  }

  private final String localPartitionRoot;
  private final int keyHashSize;
  private final int offsetSize;
  private final int bufferSize;
  private final IFetcher fetcher;
  private final ICurlyMerger curlyMerger;
  private final ICueballMerger cueballMerger;

  public CurlyUpdater(String localPartitionRoot, String remotePartitionRoot, int keyHashSize, int offsetSize, int bufferSize, IFileOps fileOps) {
    this(localPartitionRoot,
        keyHashSize,
        offsetSize,
        bufferSize,
        new Fetcher2(fileOps, new CurlyFileSelector()),
        new CurlyMerger(),
        new CueballMerger());
  }

  CurlyUpdater(String localPartitionRoot,
      int keyHashSize,
      int offsetSize,
      int bufferSize,
      IFetcher fetcher,
      ICurlyMerger curlyMerger,
      ICueballMerger cueballMerger)
  {
    this.localPartitionRoot = localPartitionRoot;
    this.keyHashSize = keyHashSize;
    this.offsetSize = offsetSize;
    this.bufferSize = bufferSize;
    this.fetcher = fetcher;
    this.curlyMerger = curlyMerger;
    this.cueballMerger = cueballMerger;
  }

  @Override
  public void update(int toVersion) throws IOException {
    // fetch all the curly and cueball files
    fetcher.fetch(getLocalVersionNumber(), toVersion);

    // figure out which curly files we want to merge
    SortedSet<String> curlyBases = new TreeSet<String>();
    SortedSet<String> curlyDeltas = new TreeSet<String>();
    getBasesAndDeltas(localPartitionRoot, curlyBases, curlyDeltas);

    // merge the latest base and all the deltas newer than it
    if (curlyBases.isEmpty()) {
      throw new IllegalStateException("There are no curly bases in "
          + localPartitionRoot + " after the fetcher ran!");
    }
    String latestCurlyBase = curlyBases.last();
    SortedSet<String> relevantCurlyDeltas = curlyDeltas.tailSet(latestCurlyBase);

    // merge the curly files
    long[] offsetAdjustments = curlyMerger.merge(latestCurlyBase, relevantCurlyDeltas);

    // figure out which cueball files we want to merge
    SortedSet<String> cueballBases = Cueball.getBases(localPartitionRoot);
    SortedSet<String> cueballDeltas = Cueball.getDeltas(localPartitionRoot);
    if (cueballBases.isEmpty()) {
      throw new IllegalStateException("There are no cueball bases in "
          + localPartitionRoot + " after the fetcher ran!");
    }
    String latestCueballBase = cueballBases.last();
    SortedSet<String> relevantCueballDeltas = cueballDeltas.tailSet(latestCurlyBase);

    if (relevantCueballDeltas.isEmpty()) {
      // no need to merge! in fact, we're done.
    } else {
      // run the cueball merger
      String newCueballBasePath = localPartitionRoot + "/"
        + String.format("%05d", Cueball.parseVersionNumber(relevantCueballDeltas.last()))
        + ".base.cueball";
      cueballMerger.merge(latestCueballBase,
          relevantCueballDeltas,
          newCueballBasePath,
          keyHashSize,
          offsetSize,
          bufferSize,
          new OffsetTransformer(keyHashSize, offsetSize, offsetAdjustments));
  
      // rename the modified base to the current version
      String newCurlyBasePath = localPartitionRoot + "/"
        + String.format("%05d", Curly.parseVersionNumber(relevantCurlyDeltas.last()))
        + ".base.curly";
      // TODO: this can fail. watch it.
      new File(latestCurlyBase).renameTo(new File(newCurlyBasePath));
    }

    // delete all the old curly bases
    deleteFiles(curlyBases, cueballBases, curlyDeltas, cueballDeltas);
  }

  private int getLocalVersionNumber() {
    File local = new File(localPartitionRoot);
    String[] filesInLocal = local.list();

    int bestVer = -1;
    if (filesInLocal != null) {
      // identify all the bases and deltas
      for (String file : filesInLocal) {
        if (file.matches(Curly.BASE_REGEX)) {
          int thisVer = Curly.parseVersionNumber(file);
          if (thisVer > bestVer) {
            bestVer = thisVer;
          }
        }
      }
    }
    return bestVer;
  }

  public static void getBasesAndDeltas(String localPartitionRoot,
      SortedSet<String> bases,
      SortedSet<String> deltas)
  {
    File local = new File(localPartitionRoot);
    String[] filesInLocal = local.list();

    if (filesInLocal != null) {
      // identify all the bases and deltas
      for (String file : filesInLocal) {
        if (file.matches(Curly.BASE_REGEX)) {
          bases.add(localPartitionRoot + "/" + file);
        }
        if (file.matches(Curly.DELTA_REGEX)) {
          deltas.add(localPartitionRoot + "/" + file);
        }
      }
    }
  }

  /**
   * Convenience method for deleting all the files in a bunch of sets
   * @param sets
   */
  private static void deleteFiles(Set<String>... sets) {
    for (Set<String> set : sets) {
      for (String s : set) {
        // TODO: if this fails, throw an exception
        new File(s).delete();
      }
    }
  }
}
