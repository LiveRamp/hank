package com.liveramp.hank.ring_group_conductor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomains;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainGroup;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.generated.ConnectedServerMetadata;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHostDomain;
import com.liveramp.hank.test.coordinator.MockRing;
import com.liveramp.hank.test.coordinator.MockRingGroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestRingGroupAutconfigure extends BaseTestCase {

  @Test
  public void testPopulateRing() throws IOException {

    RingGroupAutoconfigureTransitionFunction function = new RingGroupAutoconfigureTransitionFunction(
        2,
        Lists.newArrayList(),
        new HostReplicaStatus(0, 0, 0, 0, 0, "BUCKET")
    );

    MockRingGroup rg = new MockRingGroup(new MockDomainGroup("dg1"), "rg1", Sets.newHashSet());

    Ring ring1 = rg.addRing(0);

    Host host = ring1.addHost(new PartitionServerAddress("localhost", 12345), Lists.newArrayList());
    host.setEnvironmentFlags(Collections.singletonMap("BUCKET", "A"));

    rg.registerServer(new ConnectedServerMetadata("localhost", 12346, 0,
        Collections.singletonMap("BUCKET", "A")
    ));

    function.manageTransitions(null, rg);

    //  verify that ring has 2 hosts now
    assertEquals(
        Sets.newHashSet(
            new PartitionServerAddress("localhost", 12345),
            new PartitionServerAddress("localhost", 12346)),
        rg.getRing(0).getHosts().stream().map(Host::getAddress).collect(Collectors.toSet())
    );

  }

  @Test
  public void testPopulateRingRightBucket() throws IOException {

    RingGroupAutoconfigureTransitionFunction function = new RingGroupAutoconfigureTransitionFunction(
        2,
        Lists.newArrayList(),
        new HostReplicaStatus(0, 0, 0, 0, 0, "BUCKET")
    );

    MockRingGroup rg = new MockRingGroup(new MockDomainGroup("dg1"), "rg1", Sets.newHashSet());

    Ring ring1 = rg.addRing(0);

    Host host = ring1.addHost(new PartitionServerAddress("localhost", 12345), Lists.newArrayList());
    host.setEnvironmentFlags(Collections.singletonMap("BUCKET", "B"));

    rg.registerServer(new ConnectedServerMetadata("localhost", 12346, 0,
        Collections.singletonMap("BUCKET", "A")
    ));

    function.manageTransitions(null, rg);

    //  verify that ring still has one host
    assertEquals(
        Sets.newHashSet(
            new PartitionServerAddress("localhost", 12345)),
        rg.getRing(0).getHosts().stream().map(Host::getAddress).collect(Collectors.toSet())
    );

  }

  @Test
  public void testRemoveEmptyRings() throws IOException {

    RingGroupAutoconfigureTransitionFunction function = new RingGroupAutoconfigureTransitionFunction(
        2,
        Lists.newArrayList(),
        new HostReplicaStatus(0, 0, 0, 0, 0, "BUCKET")
    );

    MockRingGroup rg = new MockRingGroup(new MockDomainGroup("dg1"), "rg1", Sets.newHashSet());
    rg.addRing(0);

    assertNotNull(rg.getRing(0));

    function.manageTransitions(null, rg);

    assertNull(rg.getRing(0));

  }

  @Test
  public void testRemoveExcessHosts() throws IOException {

    //  1 domain
    //  3 partitions
    //  2 rings with 3 hosts each
    //  1 min replica in a bucket

    HostReplicaStatus constraints = new HostReplicaStatus(0, 1, 1, 0, 0, "BUCKET");

    RingGroupAutoconfigureTransitionFunction function = new RingGroupAutoconfigureTransitionFunction(
        2,
        Lists.newArrayList(),
        constraints
    );

    MockDomainGroup dg1 = new MockDomainGroup("dg1");
    MockRingGroup rg = new MockRingGroup(dg1, "rg1", Sets.newHashSet());


    MockDomainVersion version = new MockDomainVersion(
        0,
        System.currentTimeMillis(),
        new IncrementalDomainVersionProperties.Base()
    );

    MockDomain domain = new MockDomain("test", 0, 3, null, null, null,
        version
    );

    dg1.setDomainVersion(domain, 0);

    Ring ring0 = rg.addRing(0);
    Ring ring1 = rg.addRing(1);

    createServingHost(ring0, domain, 0);
    createServingHost(ring0, domain, 1);
    createServingHost(ring0, domain, 2);

    createServingHost(ring1, domain, 0);
    createServingHost(ring1, domain, 1);
    createServingHost(ring1, domain, 2);

    //  should remove one host from a ring
    function.removeExcessHosts(rg);

    //  should remove a differently aligned host
    function.removeExcessHosts(rg);

    //  remove nothing
    function.removeExcessHosts(rg);

    //  verify that each partition has a copy
    verifyLivenessConstraints(constraints, rg, Collections.singletonMap(domain, 3));

  }

  @Test
  public void testConsolidateRings() throws IOException {


    //  target hosts is 2
    //  3 rings
    //  1 hosts each

    HostReplicaStatus constraints = new HostReplicaStatus(0, 1, 1, 0, 0, "BUCKET");

    RingGroupAutoconfigureTransitionFunction function = new RingGroupAutoconfigureTransitionFunction(
        2,
        Lists.newArrayList(),
        constraints
    );

    MockDomainGroup dg1 = new MockDomainGroup("dg1");
    MockRingGroup rg = new MockRingGroup(dg1, "rg1", Sets.newHashSet());

    MockDomainVersion version = new MockDomainVersion(
        0,
        System.currentTimeMillis(),
        new IncrementalDomainVersionProperties.Base()
    );

    MockDomain domain = new MockDomain("test", 0, 1, null, null, null,
        version
    );

    dg1.setDomainVersion(domain, 0);

    Ring ring0 = rg.addRing(0);
    Ring ring1 = rg.addRing(1);
    Ring ring2 = rg.addRing(2);


    createServingHost(ring0, domain, 0);
    createServingHost(ring1, domain, 0);
    createServingHost(ring2, domain, 0);

    //  should remove 1 host from 1 ring
    function.consolidateRings(rg);

    //  should remove 1 host from another ring
    function.consolidateRings(rg);

    //  should remove both now-empty rings
    function.removeEmptyRings(rg);

    assertEquals(1, rg.getRings().size());

    verifyLivenessConstraints(constraints, rg, Collections.singletonMap(domain, 1));


  }

  private void verifyLivenessConstraints(
      HostReplicaStatus constraints,
      RingGroup rg,
      Map<Domain, Integer> domainsToExpectedPartitions) throws IOException {

    //  verify that globally the min replicas is respected

    int minReplicas = constraints.getMinServingReplicas();
    int minBucketReplicas = constraints.getMinServingAvailabilityBucketReplicas();

    Map<Domain, Map<Integer, Set<Host>>> replicas = PartitionUtils.domainToPartitionToHostsServing(rg, constraints);
    assertEquals(domainsToExpectedPartitions.keySet(), replicas.keySet());

    for (Domain domain : replicas.keySet()) {
      Map<Integer, Set<Host>> partitionToHosts = replicas.get(domain);

      assertEquals(partitionToHosts.keySet().size(), domainsToExpectedPartitions.get(domain).intValue());

      for (Integer partition : partitionToHosts.keySet()) {
        int liveCopies = partitionToHosts.get(partition).size();
        assertTrue(liveCopies >= minReplicas);

        System.out.println("Domain "+domain.getName()+" partition "+partition+" live: "+liveCopies +" (required: "+minReplicas+")");
      }
    }

    ThreeNestedMap<Domain, Integer, String, Long> perBucket = PartitionUtils.domainToPartitionToHostsServingInBucket(replicas, constraints);

    for (Domain domain : perBucket.key1Set()) {
      TwoNestedMap<Integer, String, Long> partitionBucketCounts = perBucket.get(domain);

      for (Integer partition : partitionBucketCounts.key1Set()) {
        for (Map.Entry<String, Long> bucketCount : partitionBucketCounts.get(partition).entrySet()) {
          Long liveCopies = bucketCount.getValue();

          assertTrue(liveCopies >= minBucketReplicas);
          System.out.println("Domain "+domain.getName()+" partition "+partition+" live: "+ liveCopies +" (required: "+minBucketReplicas+")");

        }

      }

    }

  }

  private int lastServingPort = 12000;

  private Host createServingHost(Ring ring, Domain domain, int partition) throws IOException {

    Host host = ring.addHost(new PartitionServerAddress("localhost", this.lastServingPort++), Lists.newArrayList());
    host
        .addDomain(domain)
        .addPartition(partition)
        .setCurrentDomainVersion(domain.getVersions().last().getVersionNumber());

    host.setState(HostState.SERVING);

    return host;
  }

}
