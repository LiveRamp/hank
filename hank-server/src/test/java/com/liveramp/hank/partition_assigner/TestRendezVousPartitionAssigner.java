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
    assertEquals(1, mappings.size());
    assertEquals(host1, mappings.get(0));

    mappings = getPartitionAssignment(2, host1, host2, host3);
    assertEquals(2, mappings.size());
    assertEquals(host1, mappings.get(0));
    assertEquals(host2, mappings.get(1));

    mappings = getPartitionAssignment(3, host1, host2, host3);
    assertEquals(3, mappings.size());
    assertEquals(host1, mappings.get(0));
    assertEquals(host2, mappings.get(1));
    assertEquals(host3, mappings.get(2));

    mappings = getPartitionAssignment(4, host1, host2, host3);
    assertEquals(4, mappings.size());
    assertEquals(host1, mappings.get(0));
    assertEquals(host2, mappings.get(1));
    assertEquals(host1, mappings.get(2));
    assertEquals(host3, mappings.get(3));

    mappings = getPartitionAssignment(5, host1, host2, host3);
    assertEquals(5, mappings.size());
    assertEquals(host1, mappings.get(0));
    assertEquals(host2, mappings.get(1));
    assertEquals(host1, mappings.get(2));
    assertEquals(host3, mappings.get(3));
    assertEquals(host2, mappings.get(4));

    mappings = getPartitionAssignment(6, host1, host2, host3);
    assertEquals(6, mappings.size());
    assertEquals(host1, mappings.get(0));
    assertEquals(host2, mappings.get(1));
    assertEquals(host1, mappings.get(2));
    assertEquals(host3, mappings.get(3));
    assertEquals(host2, mappings.get(4));
    assertEquals(host3, mappings.get(5));
  }

  private Map<Integer, Host> getPartitionAssignment(int numPartitions, Host... hosts) {
    SortedSet<Host> sortedHosts = new TreeSet<Host>();
    sortedHosts.addAll(Arrays.asList(hosts));
    RendezVousPartitionAssigner partitionAssigner = new RendezVousPartitionAssigner();
    Domain domain = new MockDomain("domain", 0, numPartitions, null, null, null, null);
    return partitionAssigner.getPartitionsAssignment(domain, sortedHosts);
  }
}
