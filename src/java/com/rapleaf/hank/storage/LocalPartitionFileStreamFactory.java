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

import java.io.*;

public class LocalPartitionFileStreamFactory implements PartitionFileStreamFactory {
  private final String basePath;

  public LocalPartitionFileStreamFactory(String basePath) {
    this.basePath = basePath;
  }

  @Override
  public InputStream getInputStream(int partitionNumber, String name) throws IOException {
    String path = getPath(partitionNumber, name);
    new File(new File(path).getParent()).mkdirs();
    return new FileInputStream(path);
  }

  @Override
  public OutputStream getOutputStream(int partitionNumber, String name) throws IOException {
    String path = getPath(partitionNumber, name);
    new File(new File(path).getParent()).mkdirs();
    return new FileOutputStream(path);
  }

  private String getPath(int partitionNumber, String name) {
    return basePath + "/" + partitionNumber + "/" + name;
  }
}
