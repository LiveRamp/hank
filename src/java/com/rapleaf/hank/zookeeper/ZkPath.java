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
package com.rapleaf.hank.zookeeper;

public class ZkPath {

  // Create a string representing a Zookeeper path. Each argument is a sub-directory
  // of the previous one.
  public static String create(String... parts) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < parts.length; ++i) {
      if (i != 0) {
        builder.append("/");
      }
      builder.append(parts[i]);
    }
    return builder.toString();
  }

  // Return the path's filename, i.e. the latest token in the given Zookeeper path.
  public static String filename(String path) {
    if (path == null) {
      return null;
    }
    String[] tokens = path.split("/");
    if (tokens == null) {
      return null;
    }
    return tokens[tokens.length - 1];
  }
}
