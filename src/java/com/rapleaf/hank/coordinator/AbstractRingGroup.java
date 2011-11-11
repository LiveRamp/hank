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
import java.util.SortedSet;
import java.util.TreeSet;

public abstract class AbstractRingGroup implements RingGroup {

  @Override
  public DomainGroupVersion getCurrentVersion() throws IOException {
    Integer version = getCurrentVersionNumber();
    if (version != null) {
      DomainGroup domainGroup = getDomainGroup();
      if (domainGroup != null) {
        return domainGroup.getVersionByNumber(version);
      }
    }
    return null;
  }

  @Override
  public DomainGroupVersion getUpdatingToVersion() throws IOException {
    Integer version = getUpdatingToVersionNumber();
    if (version != null) {
      DomainGroup domainGroup = getDomainGroup();
      if (domainGroup != null) {
        return domainGroup.getVersionByNumber(version);
      }
    }
    return null;
  }

  @Override
  public SortedSet<Ring> getRingsSorted() {
    return new TreeSet<Ring>(getRings());
  }

  @Override
  public void markUpdateComplete() throws IOException {
    setCurrentVersion(getUpdatingToVersionNumber());
    setUpdatingToVersion(null);
  }

  @Override
  public int compareTo(RingGroup other) {
    return getName().compareTo(other.getName());
  }

  @Override
  public String toString() {
    try {
      return "AbstractRingGroup [name=" + getName()
          + ", domain group=" + (getDomainGroup() != null ? getDomainGroup().getName() : "null")
          + ", current version=" + getCurrentVersionNumber()
          + ", updating to version=" + getUpdatingToVersionNumber()
          + "]";
    } catch (IOException e) {
      return "AbstractRingGroup [name=" + getName()
          + ", domain group=" + (getDomainGroup() != null ? getDomainGroup().getName() : "null")
          + ", current version=?"
          + ", updating to version=?"
          + "]";
    }
  }
}
