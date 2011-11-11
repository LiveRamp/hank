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

public abstract class AbstractRing implements Ring {
  private final int ringNumber;
  private final RingGroup ringGroup;

  protected AbstractRing(int ringNumber, RingGroup ringGroup) {
    this.ringNumber = ringNumber;
    this.ringGroup = ringGroup;
  }

  @Override
  public final RingGroup getRingGroup() {
    return ringGroup;
  }

  @Override
  public final int getRingNumber() {
    return ringNumber;
  }

  @Override
  public SortedSet<Host> getHostsSorted() {
    return new TreeSet<Host>(getHosts());
  }

  @Override
  public DomainGroupVersion getCurrentVersion() throws IOException {
    Integer currentVersionNumber = getCurrentVersionNumber();
    if (currentVersionNumber != null) {
      RingGroup ringGroup = getRingGroup();
      if (ringGroup != null) {
        DomainGroup domainGroup = ringGroup.getDomainGroup();
        if (domainGroup != null) {
          return domainGroup.getVersionByNumber(currentVersionNumber);
        }
      }
    }
    return null;
  }

  @Override
  public DomainGroupVersion getUpdatingToVersion() throws IOException {
    Integer updatingToVersionNumber = getUpdatingToVersionNumber();
    if (updatingToVersionNumber != null) {
      RingGroup ringGroup = getRingGroup();
      System.out.println("rg: " + ringGroup);
      if (ringGroup != null) {
        DomainGroup domainGroup = ringGroup.getDomainGroup();
      System.out.println("dg: " + domainGroup);
        if (domainGroup != null) {
          return domainGroup.getVersionByNumber(updatingToVersionNumber);
        }
      }
    }
    return null;
  }

  @Override
  public void markUpdateComplete() throws IOException {
    setCurrentVersion(getUpdatingToVersionNumber());
    setUpdatingToVersion(null);
  }

  @Override
  public int compareTo(Ring other) {
    return Integer.valueOf(ringNumber).compareTo(other.getRingNumber());
  }

  @Override
  public String toString() {
    return String.format("AbstractRing [ringGroup=%s, ring=%d, version=%d, updatingToVersion=%d]",
        (getRingGroup() != null ? getRingGroup().getName() : "null"), this.getRingNumber(), this.getCurrentVersionNumber(), this.getUpdatingToVersionNumber());
  }
}
