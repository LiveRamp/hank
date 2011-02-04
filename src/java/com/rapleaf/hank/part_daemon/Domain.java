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
package com.rapleaf.hank.part_daemon;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.Result;

public class Domain {
  private final Reader[] parts;
  private final Partitioner partitioner;

  public Domain(Reader[] partitions, Partitioner partitioner) {
    this.parts = partitions;
    this.partitioner = partitioner;
  }

  /**
   * Get the value for <i>key</i>, placing it in result.
   * @param key
   * @param result
   * @return true if this partserv is actually serving the part needed
   * @throws IOException
   */
  public boolean get(ByteBuffer key, Result result) throws IOException {
    int partition = partitioner.partition(key);
    Reader reader = parts[partition % parts.length];

    if (reader == null) {
      return false;
    }
    reader.get(key, result);
    return true;
  }
}
