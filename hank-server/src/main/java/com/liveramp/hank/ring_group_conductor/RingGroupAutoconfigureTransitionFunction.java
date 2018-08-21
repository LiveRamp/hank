package com.liveramp.hank.ring_group_conductor;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.liveramp.commons.Accessors;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.hank.config.RingGroupConfiguredDomain;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.Domains;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.generated.ConnectedServerMetadata;

public class RingGroupAutoconfigureTransitionFunction implements RingGroupTransitionFunction {
  private static final Logger LOG = LoggerFactory.getLogger(RingGroupAutoconfigureTransitionFunction.class);

  private final Integer targetHostsPerRing;
  private final HostReplicaStatus status;
  private final List<RingGroupConfiguredDomain> configuredDomains;

  public RingGroupAutoconfigureTransitionFunction(Integer targetHostsPerRing,
                                                  List<RingGroupConfiguredDomain> configuredDomains,
                                                  HostReplicaStatus status) {
    this.targetHostsPerRing = targetHostsPerRing;
    this.configuredDomains = configuredDomains;
    this.status = status;
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
  public void manageTransitions(Coordinator coordinator, RingGroup ringGroup) throws IOException {

    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHostsSorted()) {
        PartitionUtils.isFullyServing(host, true, status);
      }
    }

    //  Reconfigure rings if we changed the target # of hosts per ring

    //  if there is a ring with too many hosts, remove a host from the ring
    removeExcessHosts(ringGroup);

    //  if there are multiple rings within a bucket without enough hosts in them,
    //  choose one of them, remove a host from the one with the fewest hosts
    consolidateRings(ringGroup);

    //   if either of the above operations left an empty ring, remove it
    removeEmptyRings(ringGroup);

    //  if we can put together a full ring with unassigned hosts, do so
    assignUnassignedHostsToRings(ringGroup);

    //  configure the domains
    synchronizeDomains(coordinator, ringGroup);

    //  add domains to the ring group if possible
    addDomainsToRingGroup(coordinator, ringGroup);

  }

  private String getABForRing(Ring ring) {

    Set<String> buckets = Sets.newHashSet();
    for (Host host : ring.getHosts()) {
      buckets.add(host.getEnvironmentFlags().get(status.getAvailabilityBucketKey()));
    }

    if (buckets.size() == 1) {
      return Accessors.only(buckets);
    }

    throw new RuntimeException("Unable to find a single AB for ring: " + ring);
  }

  //      if there are multiple rings within a bucket without enough hosts in them
  //          choose one of them, remove a host from the one with the fewest hosts
  protected void consolidateRings(RingGroup ringGroup) throws IOException {

    //  split rings by AB
    Multimap<String, Ring> ringsByAB = HashMultimap.create();
    for (Ring ring : ringGroup.getRings()) {
      if (!ring.getHosts().isEmpty()) {
        ringsByAB.put(getABForRing(ring), ring);
      }
    }

    int totalRings = ringGroup.getRings().size();

    Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing =
        PartitionUtils.domainToPartitionToHostsServing(ringGroup, status);

    ThreeNestedMap<Domain, Integer, String, Long> domainPartitionBucketHostCounts =
        PartitionUtils.domainToPartitionToHostsServingInBucket(domainToPartitionToHostsFullyServing, status);

    DomainGroup domainGroup = ringGroup.getDomainGroup();

    for (String bucket : ringsByAB.keySet()) {

      Collection<Ring> rings = ringsByAB.get(bucket);

      List<Ring> smallRings = Lists.newArrayList();
      for (Ring ring : rings) {
        if (!hasEnoughHosts(ring)) {
          smallRings.add(ring);
        }
      }

      LOG.info("Found rings " + smallRings + " with fewer than target hosts");

      //  get the ring with the fewest hosts currently
      Collections.sort(smallRings, Comparator.comparingInt(o -> o.getHosts().size()));

      for (Ring smallRing : smallRings) {
        for (Host host : smallRing.getHosts()) {
          //  remove the first host with data that is over-replicated
          if (removeHostIfReplicated(totalRings, domainGroup, domainToPartitionToHostsFullyServing, domainPartitionBucketHostCounts, smallRing, host)) {
            return;
          }
        }
      }
    }
  }


  private boolean hasEnoughHosts(Ring ring) {
    return ring.getHosts().size() >= targetHostsPerRing;
  }

  protected void removeExcessHosts(RingGroup ringGroup) throws IOException {

    int totalRings = ringGroup.getRings().size();
    DomainGroup domainGroup = ringGroup.getDomainGroup();

    Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing =
        PartitionUtils.domainToPartitionToHostsServing(ringGroup, status);

    ThreeNestedMap<Domain, Integer, String, Long> domainPartitionBucketHostCounts =
        PartitionUtils.domainToPartitionToHostsServingInBucket(domainToPartitionToHostsFullyServing, status);


    for (Ring ring : ringGroup.getRings()) {
      if (ring.getHosts().size() > targetHostsPerRing) {

        if (!ring.getHosts().isEmpty()) {

          for (Host host : ring.getHosts()) {

            if (removeHostIfReplicated(totalRings, domainGroup, domainToPartitionToHostsFullyServing, domainPartitionBucketHostCounts, ring, host)) {
              return;
            }

          }
        }
      }
    }

  }

  private boolean removeHostIfReplicated(int totalRings, DomainGroup domainGroup, Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing, ThreeNestedMap<Domain, Integer, String, Long> domainPartitionBucketHostCounts, Ring ring, Host host) throws IOException {
    LiveReplicaStatus status = PartitionUtils.computeDataReplicationStatus(
        totalRings,
        domainToPartitionToHostsFullyServing,
        domainPartitionBucketHostCounts,
        domainGroup.getDomainVersions(),
        this.status,
        host
    );

    if (PartitionUtils.isFullyServing(host, false, this.status) && status == LiveReplicaStatus.OVER_REPLICATED) {

      removeHost(ring, host);

      //  just to be safe, only remove one host per iteration.  otherwise we have to carefully update the various
      //  partition maps.  I don't think this will delay things significantly.
      return true;

    }
    return false;
  }

  private void removeHost(Ring ring, Host host) throws IOException {
    // put to idle
    host.clearCommandQueue();
    Hosts.enqueueCommandIfNotPresent(host, HostCommand.GO_TO_IDLE);

    //  sleep for a few seconds
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for host to idle", e);
    }

    PartitionServerAddress address = host.getAddress();
    ring.removeHost(address);

    LOG.info("Removed host " + address + " from ring " + ring.getRingNumber());
  }

  protected void removeEmptyRings(RingGroup ringGroup) throws IOException {

    //  avoid concurrent modification exceptions
    Set<Integer> rings = ringGroup.getRings().stream().map(Ring::getRingNumber).collect(Collectors.toSet());

    for (Integer ringNum : rings) {
      Ring ring = ringGroup.getRing(ringNum);
      if (ring.getHosts().isEmpty()) {
        ringGroup.removeRing(ringNum);
      }
    }

  }

  protected void addDomainsToRingGroup(Coordinator coordinator, RingGroup ringGroup) throws IOException {


    //  for each configured domain
    for (RingGroupConfiguredDomain configuredDomain : configuredDomains) {

      Domain domain = coordinator.getDomain(configuredDomain.getName());
      DomainAndVersion version = ringGroup.getDomainGroup().getDomainVersion(domain);

      if (version == null) {

        //  either there are no versions, which is fine
        DomainVersion latestClosedVersion = Domains.getLatestVersionNotOpenNotDefunct(domain);

        if (latestClosedVersion != null) {
          int versionToAdd = latestClosedVersion.getVersionNumber();
          LOG.info("Adding domain " + domain.getName() + " version " + versionToAdd + " to domain group: " + ringGroup.getName());
          ringGroup.getDomainGroup().setDomainVersion(domain, versionToAdd);
        }

      }

    }
  }

  protected void synchronizeDomains(Coordinator coordinator, RingGroup ringGroup) throws IOException {

    for (RingGroupConfiguredDomain configuredDomain : configuredDomains) {
      Domain domain = coordinator.getDomain(configuredDomain.getName());

      //  1) domain does not exist

      Integer configuredParts = configuredDomain.getNumPartitions();
      List<String> configuredFlags = configuredDomain.getRequiredHostFlags();
      String configuredStorageEngine = configuredDomain.getStorageEngineFactory();
      String configuredPartitionerName = configuredDomain.getPartitionerName();
      Map<String, Object> configuredStorageEngineOptions = configuredDomain.getStorageEngineFactoryOptions();

      if (domain == null) {
        LOG.info("Creating configured but missing domain: " + configuredDomain.getName());

        coordinator.addDomain(
            configuredDomain.getName(),
            configuredParts,
            configuredStorageEngine,
            asYaml(configuredStorageEngineOptions),
            configuredPartitionerName,
            configuredFlags
        );

      } else {

        boolean matchesConfiguration = true;

        int numParts = domain.getNumParts();
        List<String> requiredFlags = domain.getRequiredHostFlags();
        String storageEngineName = domain.getStorageEngineFactoryClassName();
        String partitionerName = domain.getPartitionerClassName();
        Map<String, Object> storageEngineOptions = domain.getStorageEngineOptions();

        if (numParts != configuredParts) {
          LOG.info("Configured parts " + configuredParts + " does not match existing: " + numParts);
          matchesConfiguration = false;
        }

        if (!requiredFlags.equals(configuredFlags)) {
          LOG.info("Configured flags " + configuredFlags + " does not match existing: " + requiredFlags);
          matchesConfiguration = false;
        }

        if (!storageEngineName.equals(configuredStorageEngine)) {
          LOG.info("Configured storage engine " + configuredStorageEngine + " does not match existing: " + storageEngineName);
          matchesConfiguration = false;
        }

        if (!partitionerName.equals(configuredPartitionerName)) {
          LOG.info("Configured partitioner " + configuredPartitionerName + " does not match existing: " + partitionerName);
          matchesConfiguration = false;
        }

        if (!storageEngineOptions.equals(configuredStorageEngineOptions)) {
          LOG.info("Configured options " + configuredStorageEngineOptions + " does not match existing: " + storageEngineOptions);
          matchesConfiguration = false;
        }

        if (!matchesConfiguration) {
          LOG.info("Updating domain with new configurations.");
          coordinator.updateDomain(
              domain.getName(),
              configuredParts,
              configuredStorageEngine,
              asYaml(configuredStorageEngineOptions),
              configuredPartitionerName,
              configuredFlags
          );
        }

      }

    }

  }

  private String asYaml(Map<String, Object> configuredStorageEngineOptions) {
    return new Yaml().dump(configuredStorageEngineOptions);
  }

  protected Ring getUnderpopulatedRing(RingGroup group, String bucket) {

    for (Ring ring : group.getRings()) {
      Set<Host> hosts = ring.getHosts();
      if (!hosts.isEmpty()) {

        //  kinda iffy, but we don't assign multiple buckets within a ring
        String ringBucket = Accessors.first(hosts).getEnvironmentFlags().get(status.getAvailabilityBucketKey());

        if ((ringBucket == null && bucket == null) || (ringBucket != null && ringBucket.equals(bucket))) {
          if (hosts.size() < targetHostsPerRing) {
            return ring;
          }
        }

      }
    }

    return null;
  }

  protected void assignUnassignedHostsToRings(RingGroup ringGroup) throws IOException {
    if (targetHostsPerRing == null) {
      LOG.error("Cannot autoconfigure with no target hosts per ring!");
      return;
    }

    //  when there are servers registered which are not in a ring

    Multimap<String, ConnectedServerMetadata> unassignedServers = HashMultimap.create();

    for (ConnectedServerMetadata metadata : ringGroup.getLiveServers()) {
      Ring ring = ringGroup.getRingForHost(new PartitionServerAddress(metadata.get_host(), metadata.get_port_number()));
      if (ring == null) {
        String bucket = metadata.get_environment_flags().get(status.getAvailabilityBucketKey());
        unassignedServers.put(bucket, metadata);
      }
    }

    LOG.info("Found " + unassignedServers.size() + " unassigned servers in buckets: " + unassignedServers.keySet());

    for (String bucket : unassignedServers.keySet()) {
      List<ConnectedServerMetadata> initialUnassigned = Lists.newArrayList(unassignedServers.get(bucket));

      //  for each unassigned host, first see if there is a ring in the bucket which doesn't have enough hosts

      List<ConnectedServerMetadata> surplusUnassigned = Lists.newArrayList();

      for (ConnectedServerMetadata server : initialUnassigned) {
        Ring underpopulatedRing = getUnderpopulatedRing(ringGroup, bucket);

        if (underpopulatedRing != null) {
          addHost(underpopulatedRing, server);
        } else {
          surplusUnassigned.add(server);
        }
      }

      //  for everything you weren't able to assign

      int newRings = surplusUnassigned.size() / targetHostsPerRing;
      LOG.info("For bucket " + bucket + ", creating " + newRings + " new rings.");

      for (int i = 0; i < newRings; i++) {
        Ring ring = ringGroup.addRing(getNextRingNum(ringGroup));
        LOG.info("Created ring " + ring);

        List<ConnectedServerMetadata> serversInRing = surplusUnassigned.subList(i * targetHostsPerRing, (i + 1) * targetHostsPerRing);

        for (ConnectedServerMetadata server : serversInRing) {
          addHost(ring, server);
        }

      }

    }

  }

  private void addHost(Ring ring, ConnectedServerMetadata server) throws IOException {

    Host host = ring.addHost(
        new PartitionServerAddress(server.get_host(), server.get_port_number()),
        Lists.newArrayList()  //  TODO should these be associated with host config?
    );

    host.setEnvironmentFlags(server.get_environment_flags());
    LOG.info("\tadded host " + host + " to ring " + ring.getRingNumber());

  }
}
