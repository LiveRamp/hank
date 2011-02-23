package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;

public class ZkHostDomainConfig implements HostDomainConfig {

  private final ZooKeeper zk;
  private final String root;
  private final byte domainId;

  public ZkHostDomainConfig(ZooKeeper zk, String partsRoot, byte domainId) {
    this.zk = zk;
    this.domainId = domainId;
    this.root = partsRoot + "/" + (domainId & 0xff);
  }

  @Override
  public byte getDomainId() {
    return domainId;
  }

  @Override
  public Set<HostDomainPartitionConfig> getPartitions() throws IOException {
    List<String> partStrs;
    try {
      partStrs = zk.getChildren(root, false);
    } catch (Exception e) {
      throw new IOException(e);
    }
    Set<HostDomainPartitionConfig> results = new HashSet<HostDomainPartitionConfig>();
    for (String partStr : partStrs) {
      results.add(new ZkHostDomainPartitionConfig(zk, root + "/" + partStr));
    }
    return results;
  }

  public static HostDomainConfig create(ZooKeeper zk, String partsRoot, byte domainId) throws IOException {
    try {
      zk.create(partsRoot + "/" + (domainId & 0xff), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      return new ZkHostDomainConfig(zk, partsRoot, domainId);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void addPartition(int partNum, int initialVersion) throws Exception {
    ZkHostDomainPartitionConfig.create(zk, root, partNum, initialVersion);
  }
}
