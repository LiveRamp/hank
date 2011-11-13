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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

public class CurlyMerger implements ICurlyMerger {

  private static final long TRANSFER_SIZE = 32 * 1024;

  public long[] merge(final CurlyFilePath base,
                      final List<CurlyFilePath> deltas) throws IOException {
    long[] offsetAdjustments = new long[deltas.size() + 1];
    offsetAdjustments[0] = 0;

    FileChannel baseChannel = new RandomAccessFile(base.getPath(), "rw").getChannel();
    long baseLength = baseChannel.size();
    long totalOffset = baseLength;
    baseChannel.position(baseLength);

    int i = 1;
    for (CurlyFilePath delta : deltas) {
      offsetAdjustments[i] = totalOffset;

      FileChannel deltaChannel = new FileInputStream(delta.getPath()).getChannel();
      long bytesToRead = deltaChannel.size();
      totalOffset += bytesToRead;

      long total = 0;
      while (total < bytesToRead) {
        total += deltaChannel.transferTo(total, TRANSFER_SIZE, baseChannel);
      }

      deltaChannel.close();

      i++;
    }
    baseChannel.close();

    return offsetAdjustments;
  }
}
