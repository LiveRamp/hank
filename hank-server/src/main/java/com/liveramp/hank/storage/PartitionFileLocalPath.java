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

package com.liveramp.hank.storage;

import java.io.File;
import java.io.IOException;

public class PartitionFileLocalPath implements Comparable<PartitionFileLocalPath> {

  private final String path;
  private final int version;
  private final String name;

  public PartitionFileLocalPath(String path, int version) {
    this.path = path;
    this.version = version;
    this.name = new File(path).getName();
  }

  public String getPath() {
    return path;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public int compareTo(PartitionFileLocalPath other) {
    return Integer.valueOf(version).compareTo(other.version);
  }

  public static void delete(PartitionFileLocalPath file) throws IOException {
    if (!new File(file.getPath()).delete()) {
      throw new IOException("Failed to delete file " + file);
    }
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionFileLocalPath that = (PartitionFileLocalPath) o;

    if (path != null ? !path.equals(that.path) : that.path != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return path != null ? path.hashCode() : 0;
  }

  @Override
  public String toString() {
    return path;
  }
}
