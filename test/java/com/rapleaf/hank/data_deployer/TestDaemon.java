package com.rapleaf.hank.data_deployer;

import java.util.Collections;

import junit.framework.TestCase;

import com.rapleaf.hank.config.DataDeployerConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockDomainGroupConfig;
import com.rapleaf.hank.coordinator.MockDomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.MockRingGroupConfig;
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

    final MockRingGroupConfig mockRingGroupConf = new MockRingGroupConfig(null, "myRingGroup", Collections.EMPTY_SET) {
      @Override
      public DomainGroupConfig getDomainGroupConfig() {
        return domainGroupConfig;
      }

      @Override
      public int getCurrentVersion() {
        return 1;
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
      public int getCurrentVersion() {
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
