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

package com.liveramp.hank.coordinator;

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
  public int compareTo(Ring other) {
    return Integer.valueOf(ringNumber).compareTo(other.getRingNumber());
  }

  @Override
  public String toString() {
    return String.format("AbstractRing [ringGroup=%s, ring=%d]",
        (getRingGroup() != null ? getRingGroup().getName() : "null"), this.getRingNumber());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractRing)) {
      return false;
    }

    AbstractRing that = (AbstractRing)o;

    if (ringNumber != that.ringNumber) {
      return false;
    }
    if (ringGroup != null ? !ringGroup.equals(that.ringGroup) : that.ringGroup != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = ringNumber;
    result = 31 * result + (ringGroup != null ? ringGroup.hashCode() : 0);
    return result;
  }
}
