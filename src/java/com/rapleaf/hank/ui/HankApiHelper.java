package com.rapleaf.hank.ui;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.DomainVersions;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.Hosts;
import com.rapleaf.hank.coordinator.PartitionServerAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.RingGroups;
import com.rapleaf.hank.coordinator.ServingStatus;

/**
 * This class does all the logic for the HankApiServlet.
 */
public class HankApiHelper {

  public static class DomainVersionData {
    public int versionNumber;
    public long totalNumBytes;
    public long totalNumRecords;
    public boolean isClosed;
    public Long closedAt;

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("version_number", versionNumber);
      map.put("total_num_bytes", totalNumBytes);
      map.put("total_num_records", totalNumRecords);
      map.put("is_closed", isClosed);
      if (closedAt != null) map.put("closed_at", closedAt);

      return map;
    }
  }

  public static class DomainData {
    public String name;
    public int numPartitions;
    public Map<Integer, DomainVersionData> versionsMap;

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("name", name);
      map.put("num_partitions", numPartitions);

      Map<String, Object> vMap = new HashMap<String, Object>();
      for (Map.Entry<Integer, DomainVersionData> entry : versionsMap.entrySet()) {
        vMap.put(String.valueOf(entry.getKey()), entry.getValue().asMap());
      }
      map.put("versions", vMap);
      return map;
    }
  }

  public static class DomainGroupData {
    public String name;
    public Map<Integer, DomainGroupVersionData> versionsMap;
    public Map<String, RingGroupData> ringGroupsMap;

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("name", name);
      Map<String, Object> vMap = new HashMap<String, Object>();
      for (Map.Entry<Integer, DomainGroupVersionData> entry : versionsMap.entrySet()) {
        vMap.put(String.valueOf(entry.getKey()), entry.getValue().asMap());
      }
      map.put("versions", vMap);
      return map;
    }
  }

  public static class DomainGroupVersionData {
    public int version;
    public Map<String, Integer> domainVersions;

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("version_number", version);
      map.put("domain_versions", domainVersions);
      return map;
    }
  }

  public static class RingData {
    public int ringNumber;
    public Map<String, HostData> hostsMap;

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("ring_number", ringNumber);

      Map<String, Object> hMap = new HashMap<String, Object>();
      for (Map.Entry<String, HostData> entry : hostsMap.entrySet()) {
        hMap.put(entry.getKey(), entry.getValue().asMap());
      }
      map.put("hosts", hMap);
      return map;
    }
  }

  public static class RingGroupData {
    public String name;
    public Integer targetVersion;
    public boolean isRingGroupConductorOnline;
    public String domainGroupName;
    public int numPartitions;
    public int numPartitionsServedAndUpToDate;
    public Map<Integer, RingData> ringsMap;

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("name", name);
      if (targetVersion != null) map.put("target_version", targetVersion);
      map.put("is_ring_group_conductor_online", isRingGroupConductorOnline);
      map.put("domain_group", domainGroupName);
      Map<String, Object> vMap = new HashMap<String, Object>();
      for (Map.Entry<Integer, RingData> entry : ringsMap.entrySet()) {
        vMap.put(String.valueOf(entry.getKey()), entry.getValue().asMap());
      }
      map.put("rings", vMap);
      return map;
    }
  }

  public static class HostData {
    public PartitionServerAddress address;
    public HostState state;
    public boolean isOnline;

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("address", address);
      map.put("status", state.name());
      map.put("is_online", isOnline);
      return map;
    }
  }

  private final Coordinator coordinator;

  public HankApiHelper(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  protected RingGroupData getRingGroupData(RingGroup ringGroup) throws IOException {
    RingGroupData data = new RingGroupData();
    data.name = ringGroup.getName();
    data.targetVersion = ringGroup.getTargetVersionNumber();
    data.isRingGroupConductorOnline = ringGroup.isRingGroupConductorOnline();
    data.domainGroupName = ringGroup.getDomainGroup().getName();

    ServingStatus servingStatus = RingGroups.computeServingStatusAggregator(ringGroup, ringGroup.getTargetVersion()).computeServingStatus();
    data.numPartitions = servingStatus.getNumPartitions();
    data.numPartitionsServedAndUpToDate = servingStatus.getNumPartitionsServedAndUpToDate();

    Map<Integer, RingData> ringsMap = new HashMap<Integer, RingData>();
    for (Ring ring : ringGroup.getRings()) {
      ringsMap.put(ring.getRingNumber(), getRingData(ring));
    }
    data.ringsMap = ringsMap;
    return data;
  }

  protected RingData getRingData(Ring ring) throws IOException {
    RingData data = new RingData();
    data.ringNumber = ring.getRingNumber();
    Map<String, HostData> hostMap = new HashMap<String, HostData>();
    for (Host host : ring.getHosts()) {
      hostMap.put(host.getAddress().toString(), getHostData(host));
    }
    data.hostsMap = hostMap;
    return data;
  }

  protected HostData getHostData(Host host) throws IOException {
    HostData data = new HostData();
    data.address = host.getAddress();
    data.isOnline = Hosts.isOnline(host);
    data.state = host.getState();
    return data;
  }

  protected DomainGroupData getDomainGroupData(DomainGroup domainGroup) throws IOException {
    DomainGroupData data = new DomainGroupData();
    data.name = domainGroup.getName();

    Map<Integer, DomainGroupVersionData> versionsMap = new HashMap<Integer, DomainGroupVersionData>();
    for (DomainGroupVersion v : domainGroup.getVersions()) {
      versionsMap.put(v.getVersionNumber(), getDomainGroupVersionData(v));
    }
    data.versionsMap = versionsMap;

    Map<String, RingGroupData> ringGroupsMap = new HashMap<String, RingGroupData>();
    Set<RingGroup> ringGroups = coordinator.getRingGroupsForDomainGroup(domainGroup);
    for (RingGroup ringGroup : ringGroups) {
      RingGroupData rgData = getRingGroupData(ringGroup);
      ringGroupsMap.put(rgData.name, rgData);
    }

    data.ringGroupsMap = ringGroupsMap;
    return data;
  }

  protected DomainGroupVersionData getDomainGroupVersionData(DomainGroupVersion version) throws IOException {
    DomainGroupVersionData data = new DomainGroupVersionData();
    data.version = version.getVersionNumber();
    Map<String, Integer> versionsMap = new HashMap<String, Integer>();

    for (DomainGroupVersionDomainVersion v : version.getDomainVersions()) {
      versionsMap.put(v.getDomain().getName(), v.getVersion());
    }
    data.domainVersions = versionsMap;
    return data;
  }

  protected DomainVersionData getDomainVersionData(DomainVersion version) throws IOException {
    DomainVersionData data = new DomainVersionData();
    data.versionNumber = version.getVersionNumber();
    data.totalNumBytes = DomainVersions.getTotalNumBytes(version);
    data.totalNumRecords = DomainVersions.getTotalNumRecords(version);
    data.isClosed = DomainVersions.isClosed(version);
    data.closedAt = version.getClosedAt();

    return data;
  }

  protected DomainData getDomainData(Domain domain) throws IOException {
    DomainData data = new DomainData();
    data.name = domain.getName();
    data.numPartitions = domain.getNumParts();

    Map<Integer, DomainVersionData> versionsMap = new HashMap<Integer, DomainVersionData>();
    for (DomainVersion v : domain.getVersions()) {
      versionsMap.put(v.getVersionNumber(), getDomainVersionData(v));
    }
    data.versionsMap = versionsMap;
    return data;
  }

  public DomainData getDomainData(String domainName) throws IOException {
    Domain domain = coordinator.getDomain(domainName);
    if (domain != null) {
      return getDomainData(domain);
    }
    return null;
  }

}
