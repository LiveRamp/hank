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
package com.liveramp.hank.storage.mock;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.liveramp.hank.config.DataDirectoriesConfigurator;
import com.liveramp.hank.storage.CacheStatistics;
import com.liveramp.hank.storage.Reader;
import com.liveramp.hank.storage.ReaderResult;

public class MockReader implements Reader {

  private final DataDirectoriesConfigurator configurator;
  private final int partitionNumber;
  private final byte[] returnValue;
  private final Integer versionNumber;

  public MockReader(DataDirectoriesConfigurator configurator,
                    int partitionNumber,
                    byte[] returnValue,
                    Integer versionNumber) {
    this.configurator = configurator;
    this.partitionNumber = partitionNumber;
    this.returnValue = returnValue;
    this.versionNumber = versionNumber;
  }

  @Override
  public void get(ByteBuffer key, ReaderResult result) throws IOException {
    if (key.equals(ByteBuffer.wrap("nullKey".getBytes()))) {
      result.notFound();
    } else {
      result.requiresBufferSize(returnValue.length);
      result.getBuffer().position(0).limit(returnValue.length);
      result.getBuffer().put(returnValue);
      result.getBuffer().flip();
      result.found();
    }
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  @Override
  public CacheStatistics getCacheStatistics() {
    return null;
  }

  @Override
  public void close() {
  }

  public DataDirectoriesConfigurator getConfigurator() {
    return configurator;
  }

  public int getPartitionNumber() {
    return partitionNumber;
  }
}
