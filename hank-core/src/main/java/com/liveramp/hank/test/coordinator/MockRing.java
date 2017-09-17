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
package com.liveramp.hank.test.coordinator;

import com.liveramp.hank.coordinator.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

public class MockRing extends AbstractRing {

  private final Set<Host> hosts;

  public MockRing(Set<Host> hosts,
                  RingGroup ringGroup,
                  int number) {
    super(number, ringGroup);
    this.hosts = new HashSet<Host>();
    if (hosts != null) {
      this.hosts.addAll(hosts);
    }
  }

  @Override
  public Set<Host> getHosts() {
    return hosts;
  }

  @Override
  public Host addHost(PartitionServerAddress address,
                      List<String> flags) throws IOException {
    MockHost host = new MockHost(address);
    hosts.add(host);
    return host;
  }

  @Override
  public boolean removeHost(PartitionServerAddress address) {
    return hosts.remove(getHostByAddress(address));
  }

  @Override
  public Host getHostByAddress(PartitionServerAddress address) {
    for (Host host : hosts) {
      if (host.getAddress().equals(address)) {
        return host;
      }
    }
    return null;
  }

}
