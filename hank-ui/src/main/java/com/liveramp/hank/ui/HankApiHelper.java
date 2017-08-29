/**
 * Copyright 2013 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.hank.ui;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.json.JSONObject;

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.DomainVersionProperties;
import com.liveramp.hank.coordinator.DomainVersions;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.RingGroups;
import com.liveramp.hank.coordinator.ServingStatus;
import com.liveramp.hank.generated.ClientMetadata;
import com.liveramp.hank.generated.RuntimeStatisticsSummary;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;

/**
 * This class does all the logic for the HankApiServlet.
 */
public class HankApiHelper {

  private static TSerializer JSON_SERIALIZER = new TSerializer(new TSimpleJSONProtocol.Factory());

  public static class HankApiData {

    public Map<String, Object> asMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      for (Field field : getClass().getFields()) {
        try {
          String name = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());
          if (field.getType() == Map.class) {
            map.put(name, generify((Map)field.get(this)));
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
    public DomainVersionProperties properties;
    public boolean isDefunct;
  }

  public static class DomainData extends HankApiData {
    public String name;
    public int numPartitions;
    public Map<Integer, DomainVersionData> versionsMap;
    public String storageEngine;
    public Map<String, Object> storageEngineOptions;
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
    public RingGroupConductorMode ringGroupConductorMode;
    public String domainGroupName;
    public int numPartitions;
    public int numPartitionsServedAndUpToDate;
    public Map<Integer, RingData> ringsMap;
    public Map<String, ConnectedHostData> clients;
  }

  public static class ConnectedHostData extends HankApiData {
    public String host;
    public String connectedAt;
    public String type;
    public String version;

    public ConnectedHostData(String host, String connectedAt, String type, String version) {
      this.host = host;
      this.connectedAt = connectedAt;
      this.type = type;
      this.version = version;
    }
  }

  public static class HostData extends HankApiData {
    public PartitionServerAddress address;
    public HostState state;
    public boolean isOnline;
    public String statisticsString;
    public String statisticsJson;
  }

  private final Coordinator coordinator;

  public HankApiHelper(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  private static Map<String, Object> generify(Map map) {
    Map<String, Object> gMap = new HashMap<String, Object>();
    for (Object obj : map.entrySet()) {
      Map.Entry entry = (Map.Entry)obj;
      Object value = entry.getValue() instanceof HankApiData ? ((HankApiData)entry.getValue()).asMap() : entry.getValue();
      gMap.put(String.valueOf(entry.getKey()), value);
    }
    return gMap;
  }

  protected RingGroupData getRingGroupData(RingGroup ringGroup) throws IOException {
    RingGroupData data = new RingGroupData();
    data.name = ringGroup.getName();
    data.isRingGroupConductorOnline = ringGroup.isRingGroupConductorOnline();
    data.ringGroupConductorMode = ringGroup.getRingGroupConductorMode();
    data.domainGroupName = ringGroup.getDomainGroup().getName();

    ServingStatus servingStatus = RingGroups.computeServingStatusAggregator(ringGroup, ringGroup.getDomainGroup()).computeServingStatus();
    data.numPartitions = servingStatus.getNumPartitions();
    data.numPartitionsServedAndUpToDate = servingStatus.getNumPartitionsServedAndUpToDate();

    Map<Integer, RingData> ringsMap = new HashMap<Integer, RingData>();
    for (Ring ring : ringGroup.getRings()) {
      ringsMap.put(ring.getRingNumber(), getRingData(ring));
    }
    data.ringsMap = ringsMap;

    data.clients = Maps.newHashMap();
    for (ClientMetadata clientData : ringGroup.getClients()) {
      data.clients.put(clientData.get_host(), new ConnectedHostData(
          clientData.get_host(),
          Long.toString(clientData.get_connected_at()),
          clientData.get_type(),
          clientData.get_version()
      ));
    }

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
    data.statisticsString = host.getStatistic(Hosts.RUNTIME_STATISTICS_KEY);
    data.statisticsJson = getRuntimeStatsString(host);
    return data;
  }

  private String getRuntimeStatsString(Host host) throws IOException {
    RuntimeStatisticsSummary summary = host.getRuntimeStatisticsSummary();

    //  just booting up, not deployed, etc
    if (summary == null) {
      return new JSONObject().toString();
    }

    try {
      synchronized (JSON_SERIALIZER) {
        return JSON_SERIALIZER.toString(summary);
      }
    } catch (TException e) {
      throw new IOException(e);
    }

  }


  protected DomainGroupData getDomainGroupData(DomainGroup domainGroup) throws IOException {
    DomainGroupData data = new DomainGroupData();
    data.name = domainGroup.getName();
    Map<String, Integer> versionsMap = new HashMap<String, Integer>();
    for (DomainAndVersion v : domainGroup.getDomainVersions()) {
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
    data.properties = version.getProperties();
    data.isDefunct = version.isDefunct();

    return data;
  }

  protected DomainData getDomainData(Domain domain) throws IOException {
    DomainData data = new DomainData();
    data.name = domain.getName();
    data.numPartitions = domain.getNumParts();
    data.storageEngine = domain.getStorageEngineFactoryClassName();
    data.storageEngineOptions = domain.getStorageEngineOptions();

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
