package com.rapleaf.hank.ui;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.thirdparty.guava.common.base.CaseFormat;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
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

  public static class HankApiData {

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      for (Field field : getClass().getFields()) {
        try {
          if (field.getType() == Map.class) {
            map.put(field.getName(), generify((Map) field.get(this)));
          } else {
            map.put(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName()), field.get(this));
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
    public Map<Integer, DomainGroupVersionData> versionsMap;
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
    public Integer targetGroupVersion;
    public int numPartitions;
    public int numPartitionsServedAndUpToDate;
  }

  public static class DomainGroupVersionData extends HankApiData {
    public int version;
    public Map<String, Integer> domainVersions;
  }

  public static class RingData extends HankApiData {
    public int ringNumber;
    public Map<String, HostData> hostsMap;
  }

  public static class RingGroupData extends HankApiData {
    public String name;
    public Integer targetVersion;
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
    Set<DomainGroupVersion> domainGroupVersions = coordinator.getDomainGroupVersionsForDomain(domain);
    for (DomainGroupVersion domainGroupVersion : domainGroupVersions) {
      Set<RingGroup> ringGroups = coordinator.getRingGroupsForDomainGroup(domainGroupVersion.getDomainGroup());

      for (RingGroup ringGroup : ringGroups) {
        ringGroupsMap.put(ringGroup.getName(), getDomainDeployStatusForRingGroup(domainGroupVersion, domain, ringGroup));
      }
    }
    status.ringGroupsMap = ringGroupsMap;
    return status;
  }

  private DomainDeployStatusForRingGroup getDomainDeployStatusForRingGroup(DomainGroupVersion domainGroupVersion, Domain domain, RingGroup ringGroup) throws IOException {
    DomainDeployStatusForRingGroup status = new DomainDeployStatusForRingGroup();
    Integer targetDomainGroupVersion = ringGroup.getTargetVersionNumber();
    DomainGroupVersion targetVersion = targetDomainGroupVersion == null ? null : domainGroupVersion.getDomainGroup().getVersion(targetDomainGroupVersion);
    status.ringGroupName = ringGroup.getName();
    if (targetVersion != null) status.targetDomainVersion = targetVersion.getDomainVersion(domain) == null ? null : targetVersion.getDomainVersion(domain).getVersion();
    ServingStatus servingStatus = RingGroups.computeServingStatusAggregator(ringGroup, ringGroup.getTargetVersion()).computeServingStatus();
    status.numPartitions = servingStatus.getNumPartitions();
    status.numPartitionsServedAndUpToDate = servingStatus.getNumPartitionsServedAndUpToDate();
    return status;
  }

  protected DomainGroupDeployStatusForRingGroup getDomainGroupDeployStatusForRingGroup(RingGroup ringGroup) throws IOException {
    DomainGroupDeployStatusForRingGroup status = new DomainGroupDeployStatusForRingGroup();
    status.ringGroupName = ringGroup.getName();
    status.targetGroupVersion = ringGroup.getTargetVersionNumber();
    ServingStatus servingStatus = RingGroups.computeServingStatusAggregator(ringGroup, ringGroup.getTargetVersion()).computeServingStatus();
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
