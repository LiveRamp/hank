package com.liveramp.hank.partition_assigner;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHost;

import static org.junit.Assert.assertEquals;

public class TestRendezVousPartitionAssigner extends BaseTestCase {

  private final Host host1 = new MockHost(new PartitionServerAddress("host1", 0));
  private final Host host2 = new MockHost(new PartitionServerAddress("host2", 0));
  private final Host host3 = new MockHost(new PartitionServerAddress("host3", 0));

  @Test
  public void testMain() {
    Map<Integer, Host> mappings;

    mappings = getPartitionAssignment(1, host1);
    assertEquals(1, mappings.size());
    assertEquals(host1, mappings.get(0));

    mappings = getPartitionAssignment(1, host1, host2, host3);
    assertEquals(1, numPartitionsAssigned(host1, mappings));

    mappings = getPartitionAssignment(2, host1, host2, host3);
    assertEquals(2, mappings.size());
    assertEquals(1, numPartitionsAssigned(host1, mappings));
    assertEquals(0, numPartitionsAssigned(host2, mappings));
    assertEquals(1, numPartitionsAssigned(host3, mappings));

    mappings = getPartitionAssignment(3, host1, host2, host3);
    assertEquals(3, mappings.size());
    assertEquals(1, numPartitionsAssigned(host1, mappings));
    assertEquals(1, numPartitionsAssigned(host2, mappings));
    assertEquals(1, numPartitionsAssigned(host3, mappings));

    mappings = getPartitionAssignment(4, host1, host2, host3);
    assertEquals(4, mappings.size());
    assertEquals(2, numPartitionsAssigned(host1, mappings));
    assertEquals(0, numPartitionsAssigned(host2, mappings));
    assertEquals(2, numPartitionsAssigned(host3, mappings));

    mappings = getPartitionAssignment(5, host1, host2, host3);
    assertEquals(5, mappings.size());
    assertEquals(2, numPartitionsAssigned(host1, mappings));
    assertEquals(1, numPartitionsAssigned(host2, mappings));
    assertEquals(2, numPartitionsAssigned(host3, mappings));

    mappings = getPartitionAssignment(6, host1, host2, host3);
    assertEquals(6, mappings.size());
    assertEquals(2, numPartitionsAssigned(host1, mappings));
    assertEquals(2, numPartitionsAssigned(host2, mappings));
    assertEquals(2, numPartitionsAssigned(host3, mappings));
  }

  private int numPartitionsAssigned(Host host, Map<Integer, Host> mapppings) {
    int result = 0;
    for (Map.Entry<Integer, Host> entry : mapppings.entrySet()) {
      if (entry.getValue().equals(host)) {
        ++result;
      }
    }
    return result;
  }

  @Test
  public void testConsistency() {

    final int numPartitions = 1000;

    Map<Integer, Host> mappingsA = getPartitionAssignment(numPartitions, host1, host2, host3);
    assertEquals(numPartitions, mappingsA.size());

    Map<Integer, Host> mappingsB = getPartitionAssignment(numPartitions, host1, host3);
    assertEquals(numPartitions, mappingsB.size());

    int consistent = 0;
    for (int partitionId = 0; partitionId < numPartitions; ++partitionId) {
      if (mappingsA.get(partitionId).equals(mappingsB.get(partitionId))) {
        ++consistent;
      }
    }
    assertEquals(656, consistent);
  }

  private Map<Integer, Host> getPartitionAssignment(int numPartitions, Host... hosts) {
    SortedSet<Host> sortedHosts = new TreeSet<Host>();
    sortedHosts.addAll(Arrays.asList(hosts));
    RendezVousPartitionAssigner partitionAssigner = new RendezVousPartitionAssigner();
    Domain domain = new MockDomain("domain", 0, numPartitions, null, null, null, null);
    return partitionAssigner.getPartitionsAssignment(domain, sortedHosts);
  }
}
