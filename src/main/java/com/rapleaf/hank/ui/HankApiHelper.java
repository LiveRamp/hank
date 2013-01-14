package com.rapleaf.hank.ui;

import com.google.common.base.CaseFormat;
import com.rapleaf.hank.coordinator.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class does all the logic for the HankApiServlet.
 */
public class HankApiHelper {

  public static class HankApiData {

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      for (Field field : getClass().getFields()) {
        try {
          String name = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());
          if (field.getType() == Map.class) {
            map.put(name, generify((Map) field.get(this)));
          } else {
            map.put(name, field.get(this));
          }
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      }
      return map;
    }
  }

  public static class DomainVersionData extends HankApiData {
    public int versionNumber;
    public long totalNumBytes;
    public long totalNumRecords;
    public boolean isClosed;
    public Long closedAt;
  }

  public static class DomainData extends HankApiData {
    public String name;
    public int numPartitions;
    public Map<Integer, DomainVersionData> versionsMap;
  }

  public static class DomainGroupData extends HankApiData {
    public String name;
    public Map<String, Integer> domainVersions;
  }

  public static class DomainDeployStatus extends HankApiData {
    public String domainName;
    public Map<String, DomainDeployStatusForRingGroup> ringGroupsMap;
  }

  public static class DomainDeployStatusForRingGroup extends HankApiData {
    public String ringGroupName;
    public Integer targetDomainVersion;
    public int numPartitions;
    public int numPartitionsServedAndUpToDate;
  }

  public static class DomainGroupDeployStatus extends HankApiData {
    public String domainGroupName;
    public Map<String, DomainGroupDeployStatusForRingGroup> ringGroupsMap;
  }

  public static class DomainGroupDeployStatusForRingGroup extends HankApiData {
    public String ringGroupName;
    public int numPartitions;
    public int numPartitionsServedAndUpToDate;
  }

  public static class RingData extends HankApiData {
    public int ringNumber;
    public Map<String, HostData> hostsMap;
  }

  public static class RingGroupData extends HankApiData {
    public String name;
    public boolean isRingGroupConductorOnline;
    public String domainGroupName;
    public int numPartitions;
    public int numPartitionsServedAndUpToDate;
    public Map<Integer, RingData> ringsMap;
  }

  public static class HostData extends HankApiData {
    public PartitionServerAddress address;
    public HostState state;
    public boolean isOnline;
  }

  private final Coordinator coordinator;

  public HankApiHelper(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  private static Map<String, Object> generify(Map map) {
    Map<String, Object> gMap = new HashMap<String, Object>();
    for (Object obj : map.entrySet()) {
      Map.Entry entry = (Map.Entry) obj;
      Object value = entry.getValue() instanceof HankApiData ? ((HankApiData) entry.getValue()).asMap() : entry.getValue();
      gMap.put(String.valueOf(entry.getKey()), value);
    }
    return gMap;
  }

  protected RingGroupData getRingGroupData(RingGroup ringGroup) throws IOException {
    RingGroupData data = new RingGroupData();
    data.name = ringGroup.getName();
    data.isRingGroupConductorOnline = ringGroup.isRingGroupConductorOnline();
    data.domainGroupName = ringGroup.getDomainGroup().getName();

    ServingStatus servingStatus = RingGroups.computeServingStatusAggregator(ringGroup, ringGroup.getDomainGroup()).computeServingStatus();
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
    Map<String, Integer> versionsMap = new HashMap<String, Integer>();
    for (DomainGroupDomainVersion v : domainGroup.getDomainVersions()) {
      versionsMap.put(v.getDomain().getName(), v.getVersionNumber());
    }
    data.domainVersions = versionsMap;
    return data;
  }

  protected DomainGroupDeployStatus getDomainGroupDeployStatus(DomainGroup domainGroup) throws IOException {
    DomainGroupDeployStatus status = new DomainGroupDeployStatus();
    status.domainGroupName = domainGroup.getName();

    Map<String, DomainGroupDeployStatusForRingGroup> ringGroupsMap = new HashMap<String, DomainGroupDeployStatusForRingGroup>();
    Set<RingGroup> ringGroups = coordinator.getRingGroupsForDomainGroup(domainGroup);
    for (RingGroup ringGroup : ringGroups) {
      ringGroupsMap.put(ringGroup.getName(), getDomainGroupDeployStatusForRingGroup(ringGroup));
    }
    status.ringGroupsMap = ringGroupsMap;
    return status;
  }

  protected DomainDeployStatus getDomainDeployStatus(Domain domain) throws IOException {
    DomainDeployStatus status = new DomainDeployStatus();
    status.domainName = domain.getName();

    Map<String, DomainDeployStatusForRingGroup> ringGroupsMap = new HashMap<String, DomainDeployStatusForRingGroup>();
    for (RingGroup ringGroup : coordinator.getRingGroups()) {
      DomainGroup domainGroup = ringGroup.getDomainGroup();
      if (domainGroup != null && domainGroup.getDomainVersion(domain) != null) {
        ringGroupsMap.put(ringGroup.getName(), getDomainDeployStatusForRingGroup(domainGroup, domain, ringGroup));
      }
    }
    status.ringGroupsMap = ringGroupsMap;
    return status;
  }

  private DomainDeployStatusForRingGroup getDomainDeployStatusForRingGroup(DomainGroup domainGroup, Domain domain, RingGroup ringGroup) throws IOException {
    DomainDeployStatusForRingGroup status = new DomainDeployStatusForRingGroup();
    status.ringGroupName = ringGroup.getName();
    if (domainGroup != null) {
      status.targetDomainVersion = domainGroup.getDomainVersion(domain) == null ? null : domainGroup.getDomainVersion(domain).getVersionNumber();
    }
    ServingStatus servingStatus = RingGroups.computeServingStatusAggregator(ringGroup, domainGroup).computeServingStatus();
    status.numPartitions = servingStatus.getNumPartitions();
    status.numPartitionsServedAndUpToDate = servingStatus.getNumPartitionsServedAndUpToDate();
    return status;
  }

  protected DomainGroupDeployStatusForRingGroup getDomainGroupDeployStatusForRingGroup(RingGroup ringGroup) throws IOException {
    DomainGroupDeployStatusForRingGroup status = new DomainGroupDeployStatusForRingGroup();
    status.ringGroupName = ringGroup.getName();
    ServingStatus servingStatus = RingGroups.computeServingStatusAggregator(ringGroup, ringGroup.getDomainGroup()).computeServingStatus();
    status.numPartitions = servingStatus.getNumPartitions();
    status.numPartitionsServedAndUpToDate = servingStatus.getNumPartitionsServedAndUpToDate();
    return status;
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
