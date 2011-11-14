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

import com.rapleaf.hank.storage.PartitionFileLocalPath;

import java.io.File;
import java.io.IOException;

public class CurlyFilePath extends PartitionFileLocalPath {

  public CurlyFilePath(String path) {
    super(path, Curly.parseVersionNumber(new File(path).getName()));
  }

  public boolean isBase() throws IOException {
    if (getName().matches(Curly.BASE_REGEX)) {
      return true;
    } else if (getName().matches(Curly.DELTA_REGEX)) {
      return false;
    } else {
      throw new IOException("Failed to determine if file path was a base or a delta: " + getPath());
    }
  }
}
