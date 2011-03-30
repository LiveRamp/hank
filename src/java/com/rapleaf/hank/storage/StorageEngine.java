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
package com.rapleaf.hank.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.rapleaf.hank.config.PartservConfigurator;

/**
 * Defines how to read, write, and update the data stored for a given partition.
 */
public interface StorageEngine {
  public Reader getReader(PartservConfigurator configurator, int partNum) throws IOException;

  public Writer getWriter(OutputStreamFactory streamFactory, int partNum, int versionNumber, boolean base) throws IOException;

  public Updater getUpdater(PartservConfigurator configurator, int partNum) throws IOException;

  public ByteBuffer getComparableKey(ByteBuffer key);
}
