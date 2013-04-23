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
package com.liveramp.hank.zookeeper;

import java.util.ArrayList;
import java.util.List;

public class ZkPath {

  // Create a string representing a Zookeeper path. Each argument is a sub-directory
  // of the previous one.
  public static String append(String... parts) {
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
  public static String getFilename(String path) {
    if (path == null) {
      return null;
    }
    String[] tokens = path.split("/");
    if (tokens == null) {
      return null;
    }
    return tokens[tokens.length - 1];
  }

  // Return true if the given path is a hidden file, i.e. the corresponding filename
  // starts with a period.
  public static boolean isHidden(String path) {
    return getFilename(path).startsWith(".");
  }

  public static List<String> filterOutHiddenPaths(List<String> paths) {
    List<String> pathsNonHidden = new ArrayList<String>(paths.size());
    for (String path : paths) {
      if (!ZkPath.isHidden(path)) {
        pathsNonHidden.add(path);
      }
    }
    return pathsNonHidden;
  }
}
