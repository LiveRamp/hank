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

import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.rapleaf.hank.util.ReverseComparator;

public abstract class AbstractDomain implements Domain {
  public DomainVersion getOpenedVersion() throws IOException {
    if (getVersions().isEmpty()) {
      return null;
    }
    DomainVersion last = getVersions().last();
    if (last.isClosed()) {
      return null;
    } else {
      return last;
    }
  }

  @Override
  public long getTotalNumBytes() throws IOException {
    long total = 0;
    for (DomainVersion version : getVersions()) {
      if (version.isDefunct()) {
        continue;
      }
      total += version.getTotalNumBytes();
    }
    return total;
  }

  @Override
  public DomainVersion getVersionByNumber(int versionNumber) throws IOException {
    for (DomainVersion v : getVersions()) {
      if (v.getVersionNumber() == versionNumber) {
        return v;
      }
    }
    return null;
  }

  @Override
  public DomainVersion getLatestVersion() throws IOException {
    SortedSet<DomainVersion> versions = getVersions();
    if (versions == null || versions.size() == 0) {
      return null;
    } else {
      return versions.last();
    }
  }

  @Override
  public DomainVersion getLatestVersionNotOpenNotDefunct() throws IOException {
    Set<DomainVersion> originalVersions = getVersions();
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

  @Override
  public int compareTo(Domain other) {
    return getName().compareTo(other.getName());
  }

  @Override
  public boolean equals(Object other) {
    return other.getClass().equals(this.getClass())
        && this.getId() == ((AbstractDomain) other).getId();
  }

  @Override
  public String toString() {
    return String.format("AbstractDomain [id=%d, name=%s]", getId(), getName());
  }
}
