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

package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.util.ReverseComparator;

import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public final class DomainUtils {
  private DomainUtils() {}

  /**
   * @return The current opened version, null if there is none.
   */
  public static DomainVersion getOpenedVersion(Domain domain) throws IOException {
    if (domain.getVersions().isEmpty()) {
      return null;
    }
    DomainVersion last = domain.getVersions().last();
    if (last.isClosed()) {
      return null;
    } else {
      return last;
    }
  }

  /**
   * Get the sum of the num bytes used for all versions.
   * @return
   * @throws IOException
   */
  public long getTotalNumBytes(Domain domain) throws IOException {
    long total = 0;
    for (DomainVersion version : domain.getVersions()) {
      if (version.isDefunct()) {
        continue;
      }
      total += version.getTotalNumBytes();
    }
    return total;
  }

  public DomainVersion getLatestVersion(Domain domain) throws IOException {
    SortedSet<DomainVersion> versions = domain.getVersions();
    if (versions == null || versions.size() == 0) {
      return null;
    } else {
      return versions.last();
    }
  }

  public static DomainVersion getLatestVersionNotOpenNotDefunct(Domain domain) throws IOException {
    Set<DomainVersion> originalVersions = domain.getVersions();
    if (originalVersions == null || originalVersions.size() == 0) {
      return null;
    }
    SortedSet<DomainVersion> versions = new TreeSet<DomainVersion>(new ReverseComparator<DomainVersion>());
    versions.addAll(originalVersions);
    for (DomainVersion version : versions) {
      if (version.isClosed() && !version.isDefunct()) {
        return version;
      }
    }
    return null;
  }
}
