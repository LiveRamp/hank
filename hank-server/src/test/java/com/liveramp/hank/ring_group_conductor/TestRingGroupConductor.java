/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.liveramp.hank.ring_group_conductor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import org.junit.Test;

import com.liveramp.hank.config.RingGroupConductorConfigurator;
import com.liveramp.hank.config.RingGroupConfiguredDomain;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainGroup;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.test.coordinator.MockHost;
import com.liveramp.hank.test.coordinator.MockHostDomain;
import com.liveramp.hank.test.coordinator.MockHostDomainPartition;
import com.liveramp.hank.test.coordinator.MockRing;
import com.liveramp.hank.test.coordinator.MockRingGroup;

import static org.junit.Assert.assertNotNull;

public class TestRingGroupConductor {
  public class MockRingGroupUpdateTransitionFunction implements RingGroupTransitionFunction {
    public RingGroup calledWithRingGroup;

    @Override
    public void manageTransitions(Coordinator coordinator, RingGroup ringGroup) {
      calledWithRingGroup = ringGroup;
    }
  }

  public class NoOpRingGroupTransitionFunction implements RingGroupTransitionFunction {

    @Override
    public void manageTransitions(Coordinator coordinator, RingGroup ringGroup) throws IOException {
      //  no-op
    }
  }


  @Test
  public void testTriggersUpdates() throws Exception {
    final MockDomain domain = new MockDomain("domain") {
      @Override
      public DomainVersion getVersion(int version) {
        return new MockDomainVersion(0, 0l);
      }
    };

    final MockDomainGroup domainGroup = new MockDomainGroup("myDomainGroup") {
      @Override
      public Set<DomainAndVersion> getDomainVersions() {
        SortedSet<DomainAndVersion> result = new TreeSet<DomainAndVersion>();
        result.add(new DomainAndVersion(domain, 1));
        return result;
      }
    };

    final MockHostDomainPartition mockHostDomainPartition = new MockHostDomainPartition(0, 0);

    final MockHost mockHost = new MockHost(new PartitionServerAddress("locahost", 12345)) {
      @Override
      public Set<HostDomain> getAssignedDomains() throws IOException {
        return Collections.singleton((HostDomain)new MockHostDomain(domain) {
          @Override
          public HostDomainPartition addPartition(int partitionNumber) {
            return null;
          }

          @Override
          public Set<HostDomainPartition> getPartitions() {
            return Collections.singleton((HostDomainPartition)mockHostDomainPartition);
          }
        });
      }
    };

    final MockRing mockRing = new MockRing(null, null, 1) {
      @Override
      public Set<Host> getHosts() {
        return Collections.singleton((Host)mockHost);
      }
    };

    final MockRingGroup mockRingGroup = new MockRingGroup(null, "myRingGroup", Collections.<Ring>emptySet()) {
      @Override
      public DomainGroup getDomainGroup() {
        return domainGroup;
      }

      @Override
      public Set<Ring> getRings() {
        return Collections.singleton((Ring)mockRing);
      }

      @Override
      public RingGroupConductorMode getRingGroupConductorMode() {
        return RingGroupConductorMode.ACTIVE;
      }
    };

    RingGroupConductorConfigurator mockConfig = new RingGroupConductorConfigurator() {
      @Override
      public long getSleepInterval() {
        return 100;
      }

      @Override
      public RingGroupConductorMode getInitialMode() {
        return RingGroupConductorMode.ACTIVE;
      }

      @Override
      public Integer getTargetHostsPerRing() {
        return null;
      }

      @Override
      public List<RingGroupConfiguredDomain> getConfiguredDomains() {
        return Lists.newArrayList();
      }

      @Override
      public int getMinRingFullyServingObservations() {
        return 0;
      }

      @Override
      public String getHostAvailabilityBucketFlag() {
        return null;
      }

      @Override
      public int getMinServingReplicas() {
        return 2;
      }

      @Override
      public int getAvailabilityBucketMinServingReplicas() {
        return 0;
      }

      @Override
      public double getMinServingFraction() {
        return 0;
      }

      @Override
      public double getMinAvailabilityBucketServingFraction() {
        return 0;
      }

      @Override
      public String getRingGroupName() {
        return "myRingGroup";
      }

      @Override
      public Coordinator createCoordinator() {
        return new MockCoordinator() {
          @Override
          public RingGroup getRingGroup(String ringGroupName) {
            return mockRingGroup;
          }
        };
      }
    };
    MockRingGroupUpdateTransitionFunction mockTransFunc = new MockRingGroupUpdateTransitionFunction();
    RingGroupConductor daemon = new RingGroupConductor(mockConfig, mockTransFunc, new NoOpRingGroupTransitionFunction());
    daemon.processUpdates(mockRingGroup);

    assertNotNull(mockTransFunc.calledWithRingGroup);
  }
}
