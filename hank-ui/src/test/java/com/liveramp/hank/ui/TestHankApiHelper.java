package com.liveramp.hank.ui;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.liveramp.hank.test.ZkMockCoordinatorTestCase;
import com.liveramp.hank.coordinator.Coordinator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

public class TestHankApiHelper extends ZkMockCoordinatorTestCase {

  private HankApiHelper helper;
  private Coordinator coordinator;

  @Before
  public void setUp() throws Exception {
    coordinator = getApiMockCoordinator();
    helper = new HankApiHelper(coordinator);
  }

  @Test
  public void testGetDomainData() throws IOException {
    HankApiHelper.DomainData data = helper.getDomainData(coordinator.getDomain(ZkMockCoordinatorTestCase.DOMAIN_0));
    assertEquals(ZkMockCoordinatorTestCase.DOMAIN_0, data.name);
    assertEquals(32, data.numPartitions);
    assertEquals(2, data.versionsMap.size());

    HankApiHelper.DomainVersionData versionData = data.versionsMap.get(0);
    assertNotNull(versionData);
    assertEquals(0, versionData.versionNumber);
    assertEquals(true, versionData.isClosed);

    versionData = data.versionsMap.get(1);
    assertNotNull(versionData);
    assertEquals(1, versionData.versionNumber);
    assertEquals(false, versionData.isClosed);

    versionData = data.versionsMap.get(2);
    assertNull(versionData);
  }

  @Test
  public void testGetDomainGroupData() throws Exception {
    HankApiHelper.DomainGroupData data = helper.getDomainGroupData(coordinator.getDomainGroup(ZkMockCoordinatorTestCase.DOMAIN_GROUP_0));
    assertEquals(ZkMockCoordinatorTestCase.DOMAIN_GROUP_0, data.name);

    assertNotNull(data.domainVersions);
    assertEquals(1, (int)data.domainVersions.get(ZkMockCoordinatorTestCase.DOMAIN_0));
    assertEquals(1, (int)data.domainVersions.get(ZkMockCoordinatorTestCase.DOMAIN_1));
  }

  @Test
  public void testGetRingGroupData() throws Exception {
    HankApiHelper.RingGroupData data = helper.getRingGroupData(coordinator.getRingGroup(ZkMockCoordinatorTestCase.RING_GROUP_0));
    assertEquals(ZkMockCoordinatorTestCase.RING_GROUP_0, data.name);
    assertEquals(0, data.numPartitions);
    assertEquals(3, data.ringsMap.size());
  }

  @Test
  public void testGetDeployStatusForDomain() throws Exception {
    HankApiHelper.DomainDeployStatus status = helper.getDomainDeployStatus(coordinator.getDomain(ZkMockCoordinatorTestCase.DOMAIN_0));
    assertEquals(ZkMockCoordinatorTestCase.DOMAIN_0, status.domainName);
  }

  @Test
  public void testGetDeployStatusForDomainGroup() throws Exception {
    HankApiHelper.DomainGroupDeployStatus status = helper.getDomainGroupDeployStatus(coordinator.getDomainGroup(ZkMockCoordinatorTestCase.DOMAIN_GROUP_0));
    assertEquals(ZkMockCoordinatorTestCase.DOMAIN_GROUP_0, status.domainGroupName);
    assertEquals(2, status.ringGroupsMap.size());
  }
}
