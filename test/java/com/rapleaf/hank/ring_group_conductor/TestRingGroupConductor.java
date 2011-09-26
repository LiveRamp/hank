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
import com.rapleaf.hank.coordinator.VersionOrAction.Action;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainGroup;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class TestRingGroupConductor extends TestCase {
  public class MockRingGroupUpdateTransitionFunction implements RingGroupUpdateTransitionFunction {
    public RingGroup calledWithRingGroup;

    @Override
    public void manageTransitions(RingGroup ringGroup) {
      calledWithRingGroup = ringGroup;
    }
  }

  public void testTriggersUpdates() throws Exception {
    final MockDomain domain = new MockDomain("domain");

    final MockDomainGroup domainGroup = new MockDomainGroup("myDomainGroup") {
      @Override
      public DomainGroupVersion getLatestVersion() {
        return new MockDomainGroupVersion(Collections.singleton((DomainGroupVersionDomainVersion)
            new MockDomainGroupVersionDomainVersion(domain, 1)), null, 2);
      }

      @Override
      public Domain getDomain(int domainId) {
        return domain;
      }
    };

    final MockHostDomainPartition mockHostDomainPartitionConfig = new MockHostDomainPartition(0, 0,
        1);

    final MockHost mockHostConfig = new MockHost(new PartitionServerAddress("locahost", 12345)) {
      @Override
      public Set<HostDomain> getAssignedDomains() throws IOException {
        return Collections.singleton((HostDomain) new AbstractHostDomain() {
          @Override
          public HostDomainPartition addPartition(int partNum, int initialVersion) {
            return null;
          }

          @Override
          public Domain getDomain() {
            return domain;
          }

          @Override
          public Set<HostDomainPartition> getPartitions() {
            return Collections.singleton((HostDomainPartition) mockHostDomainPartitionConfig);
          }
        });
      }
    };

    final MockRing mockRingConfig = new MockRing(null, null, 1, null) {
      @Override
      public Set<Host> getHosts() {
        return Collections.singleton((Host) mockHostConfig);
      }
    };

    final MockRingGroup mockRingGroupConf = new MockRingGroup(null, "myRingGroup",
        Collections.EMPTY_SET) {
      @Override
      public DomainGroup getDomainGroup() {
        return domainGroup;
      }

      @Override
      public Integer getCurrentVersion() {
        return 1;
      }

      @Override
      public Set<Ring> getRings() {
        return Collections.singleton((Ring) mockRingConfig);
      }
    };

    RingGroupConductorConfigurator mockConfig = new RingGroupConductorConfigurator() {
      @Override
      public long getSleepInterval() {
        return 100;
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
            return mockRingGroupConf;
          }
        };
      }
    };
    MockRingGroupUpdateTransitionFunction mockTransFunc = new MockRingGroupUpdateTransitionFunction();
    RingGroupConductor daemon = new RingGroupConductor(mockConfig, mockTransFunc);
    daemon.processUpdates(mockRingGroupConf, domainGroup);

    assertNull(mockTransFunc.calledWithRingGroup);
    assertEquals(2, mockRingGroupConf.updateToVersion);
    assertEquals(Integer.valueOf(2), mockRingConfig.updatingToVersion);
    assertEquals(2, mockHostDomainPartitionConfig.updatingToVersion);
  }

  public void testProcessesUnassignDomains() throws Exception {
    final MockDomain domain = new MockDomain("domain");

    final MockDomainGroup domainGroup = new MockDomainGroup("myDomainGroup") {
      @Override
      public DomainGroupVersion getLatestVersion() {
        final MockDomainGroupVersionDomainVersion dv = new MockDomainGroupVersionDomainVersion(domain, 1) {
          @Override
          public VersionOrAction getVersionOrAction() {
            return new VersionOrAction(Action.UNASSIGN);
          }
        };

        return new MockDomainGroupVersion(
            Collections.singleton((DomainGroupVersionDomainVersion) dv), null, 2);
      }

      @Override
      public Domain getDomain(int domainId) {
        return domain;
      }
    };

    final MockHostDomainPartition mockHostDomainPartitionConfig = new MockHostDomainPartition(0, 0,
        1);

    final MockHost mockHostConfig = new MockHost(new PartitionServerAddress("locahost", 12345)) {
      @Override
      public Set<HostDomain> getAssignedDomains() throws IOException {
        return Collections.singleton((HostDomain) new AbstractHostDomain() {
          @Override
          public HostDomainPartition addPartition(int partNum, int initialVersion) {
            return null;
          }

          @Override
          public Domain getDomain() {
            return domain;
          }

          @Override
          public Set<HostDomainPartition> getPartitions() {
            return Collections.singleton((HostDomainPartition) mockHostDomainPartitionConfig);
          }
        });
      }
    };

    final MockRing mockRingConfig = new MockRing(null, null, 1, null) {
      @Override
      public Set<Host> getHosts() {
        return Collections.singleton((Host) mockHostConfig);
      }
    };

    final MockRingGroup mockRingGroupConf = new MockRingGroup(null, "myRingGroup",
        Collections.EMPTY_SET) {
      @Override
      public DomainGroup getDomainGroup() {
        return domainGroup;
      }

      @Override
      public Integer getCurrentVersion() {
        return 1;
      }

      @Override
      public Set<Ring> getRings() {
        return Collections.singleton((Ring) mockRingConfig);
      }
    };

    RingGroupConductorConfigurator mockConfig = new RingGroupConductorConfigurator() {
      @Override
      public long getSleepInterval() {
        return 100;
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
            return mockRingGroupConf;
          }
        };
      }
    };
    MockRingGroupUpdateTransitionFunction mockTransFunc = new MockRingGroupUpdateTransitionFunction();
    RingGroupConductor daemon = new RingGroupConductor(mockConfig, mockTransFunc);
    daemon.processUpdates(mockRingGroupConf, domainGroup);

    assertNull(mockTransFunc.calledWithRingGroup);
    assertEquals(2, mockRingGroupConf.updateToVersion);
    assertEquals(Integer.valueOf(2), mockRingConfig.updatingToVersion);
    assertTrue(mockHostDomainPartitionConfig.isDeletable());
  }

  public void testKeepsExistingUpdatesGoing() throws Exception {
    final MockDomainGroup domainGroup = new MockDomainGroup("myDomainGroup") {
      @Override
      public DomainGroupVersion getLatestVersion() {
        return new MockDomainGroupVersion(null, null, 2);
      }
    };

    final MockRingGroup mockRingGroupConf = new MockRingGroup(null, "myRingGroup",
        Collections.EMPTY_SET) {
      @Override
      public DomainGroup getDomainGroup() {
        return domainGroup;
      }

      @Override
      public Integer getCurrentVersion() {
        return 1;
      }

      @Override
      public boolean isUpdating() {
        return true;
      }
    };

    RingGroupConductorConfigurator mockConfig = new RingGroupConductorConfigurator() {
      @Override
      public long getSleepInterval() {
        return 100;
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
            return mockRingGroupConf;
          }
        };
      }
    };
    MockRingGroupUpdateTransitionFunction mockTransFunc = new MockRingGroupUpdateTransitionFunction();
    RingGroupConductor daemon = new RingGroupConductor(mockConfig, mockTransFunc);
    daemon.processUpdates(mockRingGroupConf, domainGroup);

    assertEquals(mockRingGroupConf, mockTransFunc.calledWithRingGroup);
  }
}
