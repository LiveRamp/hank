/**
 *  Copyright 2013 LiveRamp
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

public class HostAddress {

  private final Ring ring;
  private final PartitionServerAddress partitionServerAddress;

  public HostAddress(Ring ring, PartitionServerAddress partitionServerAddress) {
    this.ring = ring;
    this.partitionServerAddress = partitionServerAddress;
  }

  public Ring getRing() {
    return ring;
  }

  public PartitionServerAddress getPartitionServerAddress() {
    return partitionServerAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HostAddress that = (HostAddress)o;

    if (!partitionServerAddress.equals(that.partitionServerAddress)) {
      return false;
    }
    if (!ring.equals(that.ring)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = ring.hashCode();
    result = 31 * result + partitionServerAddress.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return ring + "-" + partitionServerAddress;
  }
}
