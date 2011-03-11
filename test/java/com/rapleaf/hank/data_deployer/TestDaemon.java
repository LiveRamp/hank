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
package com.rapleaf.hank.data_deployer;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import junit.framework.TestCase;

import com.rapleaf.hank.config.DataDeployerConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockDomainGroupConfig;
import com.rapleaf.hank.coordinator.MockDomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.MockHostConfig;
import com.rapleaf.hank.coordinator.MockHostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.MockRingConfig;
import com.rapleaf.hank.coordinator.MockRingGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;

public class TestDaemon extends TestCase {
  public class MockRingGroupUpdateTransitionFunction implements RingGroupUpdateTransitionFunction {
    public RingGroupConfig calledWithRingGroup;

    @Override
    public void manageTransitions(RingGroupConfig ringGroup) {
      calledWithRingGroup = ringGroup;
    }

  }

  public void testTriggersUpdates() throws Exception {
    final MockDomainGroupConfig domainGroupConfig = new MockDomainGroupConfig("myDomainGroup") {
      @Override
      public DomainGroupConfigVersion getLatestVersion() {
        return new MockDomainGroupConfigVersion(null, null, 2);
      }
    };

    final MockHostDomainPartitionConfig mockHostDomainPartitionConfig = new MockHostDomainPartitionConfig(0, 0, 1);

    final MockHostConfig mockHostConfig = new MockHostConfig(new PartDaemonAddress("locahost", 12345)) {
      @Override
      public Set<HostDomainConfig> getAssignedDomains() throws IOException {
        return Collections.singleton((HostDomainConfig)new HostDomainConfig() {
          @Override
          public HostDomainPartitionConfig addPartition(int partNum, int initialVersion) {return null;}

          @Override
          public int getDomainId() {return 0;}

          @Override
          public Set<HostDomainPartitionConfig> getPartitions() {
            return Collections.singleton((HostDomainPartitionConfig)mockHostDomainPartitionConfig);
          }
        });
      }
    };

    final MockRingConfig mockRingConfig = new MockRingConfig(null, null, 1, null) {
      @Override
      public Set<HostConfig> getHosts() {
        return Collections.singleton((HostConfig)mockHostConfig);
      }
    };

    final MockRingGroupConfig mockRingGroupConf = new MockRingGroupConfig(null, "myRingGroup", Collections.EMPTY_SET) {
      @Override
      public DomainGroupConfig getDomainGroupConfig() {
        return domainGroupConfig;
      }

      @Override
      public Integer getCurrentVersion() {
        return 1;
      }

      @Override
      public Set<RingConfig> getRingConfigs() {
        return Collections.singleton((RingConfig)mockRingConfig);
      }
    };

    DataDeployerConfigurator mockConfig = new DataDeployerConfigurator() {
      @Override
      public long getSleepInterval() {
        return 100;
      }

      @Override
      public String getRingGroupName() {
        return "myRingGroup";
      }

      @Override
      public Coordinator getCoordinator() {
        return new MockCoordinator(){
          @Override
          public RingGroupConfig getRingGroupConfig(String ringGroupName) {
            return mockRingGroupConf;
          }
        };
      }
    };
    MockRingGroupUpdateTransitionFunction mockTransFunc = new MockRingGroupUpdateTransitionFunction();
    Daemon daemon = new Daemon(mockConfig, mockTransFunc);
    daemon.processUpdates(mockRingGroupConf, domainGroupConfig);

    assertNull(mockTransFunc.calledWithRingGroup);
    assertEquals(2, mockRingGroupConf.updateToVersion);
    assertEquals(Integer.valueOf(2), mockRingConfig.updatingToVersion);
    assertEquals(2, mockHostDomainPartitionConfig.updatingToVersion);
  }

  public void testKeepsExistingUpdatesGoing() throws Exception {
    final MockDomainGroupConfig domainGroupConfig = new MockDomainGroupConfig("myDomainGroup") {
      @Override
      public DomainGroupConfigVersion getLatestVersion() {
        return new MockDomainGroupConfigVersion(null, null, 2);
      }
    };

    final MockRingGroupConfig mockRingGroupConf = new MockRingGroupConfig(null, "myRingGroup", Collections.EMPTY_SET) {
      @Override
      public DomainGroupConfig getDomainGroupConfig() {
        return domainGroupConfig;
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

    DataDeployerConfigurator mockConfig = new DataDeployerConfigurator() {
      @Override
      public long getSleepInterval() {
        return 100;
      }

      @Override
      public String getRingGroupName() {
        return "myRingGroup";
      }

      @Override
      public Coordinator getCoordinator() {
        return new MockCoordinator(){
          @Override
          public RingGroupConfig getRingGroupConfig(String ringGroupName) {
            return mockRingGroupConf;
          }
        };
      }
    };
    MockRingGroupUpdateTransitionFunction mockTransFunc = new MockRingGroupUpdateTransitionFunction();
    Daemon daemon = new Daemon(mockConfig, mockTransFunc);
    daemon.processUpdates(mockRingGroupConf, domainGroupConfig);

    assertEquals(mockRingGroupConf, mockTransFunc.calledWithRingGroup);
  }
}
