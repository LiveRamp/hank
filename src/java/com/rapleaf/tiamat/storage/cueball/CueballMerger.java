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
package com.rapleaf.tiamat.storage.cueball;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.SortedSet;


public final class CueballMerger implements ICueballMerger {

  public void merge(final String latestBase,
      final SortedSet<String> deltas,
      final String newBasePath,
      final int keyHashSize,
      final int valueSize,
      int bufferSize,
      ValueTransformer transformer)
  throws IOException {
    StreamBuffer[] sbs = new StreamBuffer[deltas.size() + 1];

    // open the current base
    StreamBuffer base = new StreamBuffer(new FileInputStream(latestBase), 0,
        keyHashSize, valueSize, bufferSize);
    sbs[0] = base;

    // open all the deltas
    int i = 1;
    for (String deltaPath : deltas) {
      StreamBuffer db = new StreamBuffer(new FileInputStream(deltaPath), i,
          keyHashSize, valueSize, bufferSize);
      sbs[i++] = db;
    }

    OutputStream newBaseStream = new BufferedOutputStream(
        new FileOutputStream(newBasePath), bufferSize);

    while (true) {
      StreamBuffer least = null;
      for (i = 0; i < sbs.length; i++) {
        boolean remaining = sbs[i].anyRemaining();
        if (remaining) {
          if (least == null) {
            least = sbs[i];
          } else {
            int comparison = least.compareTo(sbs[i]);
            if (comparison == 0) {
              least.consume();
              least = sbs[i];
            } else if (comparison == 1) {
              least = sbs[i];
            }
          }
        }
      }

      if (least == null) {
        break;
      }

      if (transformer != null) {
        transformer.transform(least);
      }
      newBaseStream.write(least.getBuffer(), least.getCurrentOffset(), valueSize + keyHashSize);
      least.consume();
    }

    for (StreamBuffer sb : sbs) {
      sb.close();
    }
    newBaseStream.flush();
    newBaseStream.close();
  }
}
