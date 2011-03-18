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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LocalDiskOutputStreamFactory implements OutputStreamFactory {
  private final String basePath;

  public LocalDiskOutputStreamFactory(String basePath) {
    this.basePath = basePath;
  }

  @Override
  public OutputStream getOutputStream(int partNum, String name) throws IOException {
    String fullPath = basePath + "/" + partNum + "/" + name;
    new File(new File(fullPath).getParent()).mkdirs();
    return new FileOutputStream(fullPath);
  }
}