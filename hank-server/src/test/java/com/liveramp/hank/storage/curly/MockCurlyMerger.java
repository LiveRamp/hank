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

import java.io.IOException;
import java.util.List;

public class MockCurlyMerger implements ICurlyMerger {

  public CurlyFilePath latestBase;
  public List<String> deltaRemoteFiles;

  @Override
  public long[] merge(CurlyFilePath latestBase,
                      List<String> deltaRemoteFiles,
                      PartitionRemoteFileOps partitionRemoteFileOps) throws IOException {
    this.latestBase = latestBase;
    this.deltaRemoteFiles = deltaRemoteFiles;
    return null;
  }
}
