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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PartitionServerAddress implements Comparable<PartitionServerAddress> {

  private static final Pattern HOST_AND_PORT_PATTERN = Pattern.compile("([^:]+):(\\d+)");
  private final String hostName;
  private final int portNumber;

  public PartitionServerAddress(String hostName, int portNumber) {
    this.hostName = hostName;
    this.portNumber = portNumber;
  }

  public String getHostName() {
    return hostName;
  }

  public int getPortNumber() {
    return portNumber;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
    result = prime * result + portNumber;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PartitionServerAddress other = (PartitionServerAddress) obj;
    if (hostName == null) {
      if (other.hostName != null) {
        return false;
      }
    } else if (!hostName.equals(other.hostName)) {
      return false;
    }
    if (portNumber != other.portNumber) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return hostName + ":" + portNumber;
  }

  public static PartitionServerAddress parse(String s) {
    Matcher matcher = HOST_AND_PORT_PATTERN.matcher(s);
    if (matcher.matches()) {
      return new PartitionServerAddress(matcher.group(1), Integer.parseInt(matcher.group(2)));
    } else {
      throw new RuntimeException(s + " is not a properly formatted host:port pair.");
    }
  }

  @Override
  public int compareTo(PartitionServerAddress arg0) {
    int hostComparison = getHostName().compareTo(arg0.getHostName());
    if (hostComparison == 0) {
      return Integer.valueOf(getPortNumber()).compareTo(arg0.getPortNumber());
    }
    return hostComparison;
  }
}
