package com.liveramp.hank.ring_group_conductor;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.liveramp.hank.config.RingGroupConfiguredDomain;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.Domains;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.generated.ConnectedServerMetadata;

public class RingGroupAutoconfigureTransitionFunction implements RingGroupTransitionFunction {
  private static final Logger LOG = LoggerFactory.getLogger(RingGroupAutoconfigureTransitionFunction.class);

  private final Integer targetHostsPerRing;
  private final String availabilityBucketFlag;
  private final List<RingGroupConfiguredDomain> configuredDomains;

  public RingGroupAutoconfigureTransitionFunction(Integer targetHostsPerRing,
                                                  List<RingGroupConfiguredDomain> configuredDomains,
                                                  String availablilityBucketFlag) {
    this.targetHostsPerRing = targetHostsPerRing;
    this.configuredDomains = configuredDomains;
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
  public void manageTransitions(Coordinator coordinator, RingGroup ringGroup) throws IOException {

    assignUnassignedHostsToRings(ringGroup);

    synchronizeDomains(coordinator, ringGroup);

    addDomainsToRingGroup(coordinator, ringGroup);

  }

  protected void addDomainsToRingGroup(Coordinator coordinator, RingGroup ringGroup) throws IOException {


    //  for each configured domain
    for (RingGroupConfiguredDomain configuredDomain : configuredDomains) {

      Domain domain = coordinator.getDomain(configuredDomain.getName());
      DomainAndVersion version = ringGroup.getDomainGroup().getDomainVersion(domain);

      if(version == null){

        //  either there are no versions, which is fine
        DomainVersion latestClosedVersion = Domains.getLatestVersionNotOpenNotDefunct(domain);

        if(latestClosedVersion != null){
          int versionToAdd = latestClosedVersion.getVersionNumber();
          LOG.info("Adding domain "+domain.getName()+" version "+versionToAdd+" to domain group: "+ringGroup.getName());
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
        LOG.info("Creating configured but missing domain: "+configuredDomain.getName());

        coordinator.addDomain(
            configuredDomain.getName(),
            configuredParts,
            configuredStorageEngine,
            asYaml(configuredStorageEngineOptions),
            configuredPartitionerName,
            configuredFlags
        );

      }

      else {

        boolean matchesConfiguration = true;

        int numParts = domain.getNumParts();
        List<String> requiredFlags = domain.getRequiredHostFlags();
        String storageEngineName = domain.getStorageEngineFactoryClassName();
        String partitionerName = domain.getPartitionerClassName();
        Map<String, Object> storageEngineOptions = domain.getStorageEngineOptions();

        if(numParts != configuredParts){
          LOG.info("Configured parts "+ configuredParts +" does not match existing: "+ numParts);
          matchesConfiguration = false;
        }

        if(!requiredFlags.equals(configuredFlags)){
          LOG.info("Configured flags "+configuredFlags +" does not match existing: "+requiredFlags);
          matchesConfiguration = false;
        }

        if(!storageEngineName.equals(configuredStorageEngine)){
          LOG.info("Configured storage engine "+configuredStorageEngine+" does not match existing: "+storageEngineName);
          matchesConfiguration = false;
        }

        if(!partitionerName.equals(configuredPartitionerName)){
          LOG.info("Configured partitioner "+configuredPartitionerName+" does not match existing: "+partitionerName);
          matchesConfiguration = false;
        }

        if(!storageEngineOptions.equals(configuredStorageEngineOptions)){
          LOG.info("Configured options "+configuredStorageEngineOptions+" does not match existing: "+storageEngineOptions);
        }

        if(!matchesConfiguration) {
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


  protected void assignUnassignedHostsToRings(RingGroup ringGroup) throws IOException {
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
