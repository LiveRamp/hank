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
package com.rapleaf.hank.storage.constant;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.Writer;

public class ConstantStorageEngine implements StorageEngine {
  public static class Factory implements StorageEngineFactory {
    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, String domainName) throws IOException {
      return new ConstantStorageEngine(options);
    }

    @Override
    public String getPrettyName() {
      return "Constant";
    }
  }

  public ConstantStorageEngine(Map<String, Object> options) {
  }

  @Override
  public Reader getReader(PartservConfigurator configurator, int partNum) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Updater getUpdater(PartservConfigurator configurator, int partNum) {
    throw new NotImplementedException();
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partNum, int versionNumber, boolean base) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    throw new UnsupportedOperationException();
  }
}
