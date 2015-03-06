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

import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.util.IOStreamUtils;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class CurlyMerger implements ICurlyMerger {

  private static final Logger LOG = LoggerFactory.getLogger(CurlyMerger.class);

  @Override
  public long[] merge(final CurlyFilePath base,
                      final List<String> deltaRemoteFiles,
                      final PartitionRemoteFileOps partitionRemoteFileOps) throws IOException {
    long[] offsetAdjustments = new long[deltaRemoteFiles.size() + 1];
    offsetAdjustments[0] = 0;

    // Open the base in append mode
    File baseFile = new File(base.getPath());
    FileOutputStream baseOutputStream = new FileOutputStream(baseFile, true);
    try {
      // Loop over deltas and append them to the base in order, keeping track of offset adjustments
      long totalOffset = baseFile.length();
      int i = 1;
      for (String deltaRemoteFile : deltaRemoteFiles) {
        offsetAdjustments[i] = totalOffset;
        InputStream deltaRemoteInputStream = partitionRemoteFileOps.getInputStream(deltaRemoteFile);
        try {
          LOG.info("Merging remote file " + deltaRemoteFile + " into file " + base.getPath());
          long bytesCopied = IOStreamUtils.copy(deltaRemoteInputStream, baseOutputStream);
          totalOffset += bytesCopied;
        } finally {
          deltaRemoteInputStream.close();
        }
        i++;
      }
    } finally {
      // Close base streams
      baseOutputStream.close();
    }
    return offsetAdjustments;
  }
}
