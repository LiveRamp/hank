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
package com.rapleaf.hank.ring_group_conductor;

import com.rapleaf.hank.config.RingGroupConductorConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainGroup;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class TestRingGroupConductor extends TestCase {
  public class MockRingGroupUpdateTransitionFunction implements RingGroupUpdateTransitionFunction {
    public RingGroup calledWithRingGroup;

    @Override
    public void manageTransitions(RingGroup ringGroup) {
      calledWithRingGroup = ringGroup;
    }
  }

  public void testTriggersUpdates() throws Exception {
    final MockDomain domain = new MockDomain("domain") {
      @Override
      public DomainVersion getVersionByNumber(int version) {
        return new MockDomainVersion(0, 0l);
      }
    };

    final MockDomainGroup domainGroup = new MockDomainGroup("myDomainGroup") {
      final SortedSet<DomainGroupVersion> versions = new TreeSet<DomainGroupVersion>() {{
        add(new MockDomainGroupVersion(Collections.singleton((DomainGroupVersionDomainVersion)
            new MockDomainGroupVersionDomainVersion(domain, 1)), null, 2));
      }};

      @Override
      public SortedSet<DomainGroupVersion> getVersions() {
        return versions;
      }

      @Override
      public Domain getDomain(int domainId) {
        return domain;
      }
    };

    final MockHostDomainPartition mockHostDomainPartition = new MockHostDomainPartition(0, 0);

    final MockHost mockHost = new MockHost(new PartitionServerAddress("locahost", 12345)) {
      @Override
      public Set<HostDomain> getAssignedDomains() throws IOException {
        return Collections.singleton((HostDomain) new MockHostDomain(domain) {
          @Override
          public HostDomainPartition addPartition(int partNum) {
            return null;
          }

          @Override
          public Set<HostDomainPartition> getPartitions() {
            return Collections.singleton((HostDomainPartition) mockHostDomainPartition);
          }
        });
      }
    };

    final MockRing mockRing = new MockRing(null, null, 1, null) {
      @Override
      public Set<Host> getHosts() {
        return Collections.singleton((Host) mockHost);
      }
    };

    final MockRingGroup mockRingGroup = new MockRingGroup(null, "myRingGroup", Collections.<Ring>emptySet(), 1) {
      @Override
      public DomainGroup getDomainGroup() {
        return domainGroup;
      }

      @Override
      public Set<Ring> getRings() {
        return Collections.singleton((Ring) mockRing);
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
    RingGroupConductor daemon = new RingGroupConductor(mockConfig, mockTransFunc);
    daemon.processUpdates(mockRingGroup, domainGroup);

    assertNotNull(mockTransFunc.calledWithRingGroup);

    assertEquals(Integer.valueOf(2), mockRingGroup.targetVersion);
  }

  public void testKeepsExistingUpdatesGoing() throws Exception {
    final MockDomainGroup domainGroup = new MockDomainGroup("myDomainGroup") {
      final SortedSet<DomainGroupVersion> versions = new TreeSet<DomainGroupVersion>() {{
        add(new MockDomainGroupVersion(null, null, 2));
      }};

      @Override
      public SortedSet<DomainGroupVersion> getVersions() {
        return versions;
      }
    };

    final MockRingGroup mockRingGroup = new MockRingGroup(null, "myRingGroup", Collections.<Ring>emptySet(), null) {
      @Override
      public DomainGroup getDomainGroup() {
        return domainGroup;
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
    RingGroupConductor daemon = new RingGroupConductor(mockConfig, mockTransFunc);
    daemon.processUpdates(mockRingGroup, domainGroup);

    assertEquals(mockRingGroup, mockTransFunc.calledWithRingGroup);
  }
}
