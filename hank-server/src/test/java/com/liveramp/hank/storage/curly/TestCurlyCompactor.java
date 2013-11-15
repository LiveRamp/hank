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
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.storage.LocalPartitionRemoteFileOps;
import com.liveramp.hank.storage.Writer;
import com.liveramp.hank.storage.cueball.CueballFilePath;
import com.liveramp.hank.storage.cueball.ICueballStreamBufferMergeSortFactory;
import com.liveramp.hank.storage.cueball.IKeyFileStreamBufferMergeSort;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.storage.incremental.IncrementalPartitionUpdaterTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCurlyCompactor extends IncrementalPartitionUpdaterTestCase {

  private final DomainVersion v0 = new MockDomainVersion(0, 0l, new IncrementalDomainVersionProperties.Base());
  private final DomainVersion v1 = new MockDomainVersion(1, 0l, new IncrementalDomainVersionProperties.Delta(0));
  private final DomainVersion v2 = new MockDomainVersion(2, 0l, new IncrementalDomainVersionProperties.Delta(1));
  private final Domain domain = new MockDomain("domain") {
    @Override
    public DomainVersion getVersion(int versionNumber) {
      switch (versionNumber) {
        case 0:
          return v0;
        case 1:
          return v1;
        case 2:
          return v2;
        default:
          throw new RuntimeException("Unknown version: " + versionNumber);
      }
    }
  };

  private CurlyCompactor compactor;

  private boolean mergerCalled = false;
  private boolean mergeSortBufferCalled = false;

  @Before
  public void setUp() throws Exception {

    ICurlyCompactingMerger merger = new ICurlyCompactingMerger() {
      @Override
      public void merge(CurlyFilePath curlyBasePath,
                        List<CurlyFilePath> curlyDeltas,
                        IKeyFileStreamBufferMergeSort keyFileStreamBufferMergeSort,
                        ICurlyReaderFactory curlyReaderFactory,
                        Writer recordFileWriter) throws IOException {
        assertEquals("Correct base version", 0, curlyBasePath.getVersion());
        assertEquals("Correct number of deltas used by merge", 2, curlyDeltas.size());
        assertEquals("Correct delta version", 1, curlyDeltas.get(0).getVersion());
        assertEquals("Correct delta version", 2, curlyDeltas.get(1).getVersion());
        mergerCalled = true;
      }
    };

    ICueballStreamBufferMergeSortFactory cueballStreamBufferMergeSortFactory =
        new ICueballStreamBufferMergeSortFactory() {
          @Override
          public IKeyFileStreamBufferMergeSort getInstance(CueballFilePath cueballBase,
                                                           List<CueballFilePath> cueballDeltas) {
            assertEquals("Correct base version", 0, cueballBase.getVersion());
            assertEquals("Correct number of deltas used by merge sort", 2, cueballDeltas.size());
            assertEquals("Correct delta version", 1, cueballDeltas.get(0).getVersion());
            assertEquals("Correct delta version", 2, cueballDeltas.get(1).getVersion());
            mergeSortBufferCalled = true;
            return null;
          }
        };

    this.compactor = new CurlyCompactor(domain,
        new LocalPartitionRemoteFileOps(remotePartitionRoot, 0),
        localPartitionRoot,
        merger,
        cueballStreamBufferMergeSortFactory,
        null);

    if (!new File(updateWorkRoot).mkdir()) {
      throw new IOException("Failed to create update work root");
    }
  }

  @Test
  public void testUpdate() throws IOException {
    // Success merging with deltas
    assertFalse(existsUpdateWorkFile("00002.base.cueball"));
    assertFalse(existsUpdateWorkFile("00002.base.curly"));
    makeLocalFile("00000.base.cueball");
    makeLocalFile("00000.base.curly");
    makeLocalCacheFile("00001.delta.cueball");
    makeLocalCacheFile("00001.delta.curly");
    makeLocalCacheFile("00002.delta.cueball");
    makeLocalCacheFile("00002.delta.curly");

    // Make sure file exists on remote partition so that the versions are not considered empty
    makeRemoteFile("0/00001.delta.cueball");
    makeRemoteFile("0/00001.delta.curly");
    makeRemoteFile("0/00002.delta.cueball");
    makeRemoteFile("0/00002.delta.curly");

    compactor.compact(v2, new Writer() {
      @Override
      public void write(ByteBuffer key, ByteBuffer value) throws IOException {
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public long getNumBytesWritten() {
        return 0;
      }

      @Override
      public long getNumRecordsWritten() {
        return 0;
      }
    });

    assertTrue(mergerCalled);
    assertTrue(mergeSortBufferCalled);
  }
}
