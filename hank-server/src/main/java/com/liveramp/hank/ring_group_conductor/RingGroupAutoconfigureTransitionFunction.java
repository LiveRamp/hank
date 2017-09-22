package com.liveramp.hank.ring_group_conductor;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.generated.ConnectedServerMetadata;

public class RingGroupAutoconfigureTransitionFunction implements RingGroupTransitionFunction {
  private static final Logger LOG = LoggerFactory.getLogger(RingGroupAutoconfigureTransitionFunction.class);

  private final Integer targetHostsPerRing;
  private final String availabilityBucketFlag;

  public RingGroupAutoconfigureTransitionFunction(Integer targetHostsPerRing,
                                                  String availablilityBucketFlag) {
    this.targetHostsPerRing = targetHostsPerRing;
    this.availabilityBucketFlag = availablilityBucketFlag;
  }

  private int getNextRingNum(RingGroup group) {
    return group.getRings()
        .stream()
        .map(ring -> ring.getRingNumber())
        .max(Comparator.naturalOrder())
        .orElse(-1)
        + 1;
  }

  @Override
  public void manageTransitions(RingGroup ringGroup) throws IOException {

    if (targetHostsPerRing == null) {
      LOG.error("Cannot autoconfigure with no target hosts per ring!");
      return;
    }

    //  1) when there are servers registered which are not in a ring

    Multimap<String, ConnectedServerMetadata> unassignedServers = HashMultimap.create();

    for (ConnectedServerMetadata metadata : ringGroup.getLiveServers()) {
      Ring ring = ringGroup.getRingForHost(new PartitionServerAddress(metadata.get_host(), metadata.get_port_number()));
      if (ring == null) {
        String bucket = metadata.get_environment_flags().get(availabilityBucketFlag);
        unassignedServers.put(bucket, metadata);
      }
    }

    LOG.info("Found " + unassignedServers.size() + " unassigned servers in buckets: " + unassignedServers.keySet());

    for (String bucket : unassignedServers.keySet()) {
      List<ConnectedServerMetadata> servers = Lists.newArrayList(unassignedServers.get(bucket));

      int newRings = servers.size() / targetHostsPerRing;
      LOG.info("For bucket " + bucket + ", creating " + newRings + " new rings.");

      for (int i = 0; i < newRings; i++) {
        Ring ring = ringGroup.addRing(getNextRingNum(ringGroup));
        LOG.info("Created ring " + ring);

        List<ConnectedServerMetadata> serversInRing = servers.subList(i * targetHostsPerRing, (i + 1) * targetHostsPerRing);

        for (ConnectedServerMetadata server : serversInRing) {

          Host host = ring.addHost(
              new PartitionServerAddress(server.get_host(), server.get_port_number()),
              Lists.newArrayList()  //  TODO should these be associated with host config?
          );

          LOG.info("\tadded host " + host + " to ring " + ring.getRingNumber());

        }

      }


    }

  }
}
