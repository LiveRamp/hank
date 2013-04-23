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
package com.liveramp.hank.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public final class FsUtils {
  private FsUtils() {
  }

  public static void rmrf(String dir) {
    File d = new File(dir);
    if (d.exists()) {
      if (d.isDirectory()) {
        for (String x : d.list()) {
          rmrf(dir + "/" + x);
        }
      }
      if (!d.delete()) {
        throw new RuntimeException("tried to delete " + dir + " but failed!");
      }
    }
  }

  public static Set<String> getMatchingPaths(String regex, String... dirs) throws IOException {
    Set<String> matches = new HashSet<String>();
    for (String dir : dirs) {
      File local = new File(dir);
      if (!local.isDirectory()) {
        throw new IOException(dir + " does not exist or is not a directory!");
      }
      String[] filesInLocal = local.list();

      if (filesInLocal == null) {
        throw new IOException(dir + " does not exist or is not a directory!");
      }

      for (String file : filesInLocal) {
        if (file.matches(regex)) {
          matches.add(dir + "/" + file);
        }
      }
    }
    return matches;
  }

  public static String readFileToString(File file) throws IOException {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line = br.readLine();
    while (line != null) {
      sb.append(line);
      sb.append("\n");
      line = br.readLine();
    }
    return sb.toString();
  }
}
