package com.liveramp.hank.ui;

import com.liveramp.hank.ZkMockCoordinatorTestCase;
import com.liveramp.hank.coordinator.Coordinator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;

public class TestHankApiHelper extends ZkMockCoordinatorTestCase {

  private HankApiHelper helper;
  private Coordinator coordinator;

  @Before
  public void setUp() throws Exception {
    coordinator = getMockCoordinator();
    helper = new HankApiHelper(coordinator);
  }

  @Test
  public void testGetDomainData() throws IOException {
    HankApiHelper.DomainData data = helper.getDomainData(coordinator.getDomain("domain0"));
    assertEquals("domain0", data.name);
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
    HankApiHelper.DomainGroupData data = helper.getDomainGroupData(coordinator.getDomainGroup("Group_1"));
    assertEquals("Group_1", data.name);

    assertNotNull(data.domainVersions);
    assertEquals(1, (int) data.domainVersions.get("domain0"));
    assertEquals(1, (int) data.domainVersions.get("domain1"));
  }

  @Test
  public void testGetRingGroupData() throws Exception {
    HankApiHelper.RingGroupData data = helper.getRingGroupData(coordinator.getRingGroup("RG_Alpha"));
    assertEquals("RG_Alpha", data.name);
    assertEquals(0, data.numPartitions);
    assertEquals(3, data.ringsMap.size());
  }

  @Test
  public void testGetDeployStatusForDomain() throws Exception {
    HankApiHelper.DomainDeployStatus status = helper.getDomainDeployStatus(coordinator.getDomain("domain0"));
    assertEquals("domain0", status.domainName);
  }

  @Test
  public void testGetDeployStatusForDomainGroup() throws Exception {
    HankApiHelper.DomainGroupDeployStatus status = helper.getDomainGroupDeployStatus(coordinator.getDomainGroup("Group_1"));
    assertEquals("Group_1", status.domainGroupName);
    assertEquals(2, status.ringGroupsMap.size());
  }
}
