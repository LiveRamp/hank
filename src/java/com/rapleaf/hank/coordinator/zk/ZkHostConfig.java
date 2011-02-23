package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.PartDaemonState;
import com.rapleaf.hank.coordinator.UpdateDaemonState;

public class ZkHostConfig implements HostConfig {
  private class StateChangeWatcher implements Watcher {
    private final HostStateChangeListener listener;

    public StateChangeWatcher(HostStateChangeListener l) {
      listener = l;
    }

    public void process(WatchedEvent event) {
      switch (event.getType()) {
        case NodeDataChanged:
          listener.stateChange(ZkHostConfig.this);
          // reset callback
          try {
            setWatch();
          } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
      }
    }

    private void setWatch() throws KeeperException, InterruptedException {
      zk.getData(hostPath + "/part_daemon/status", this, new Stat());
      zk.getData(hostPath + "/update_daemon/status", this, new Stat());
    }
  }

  private final ZooKeeper zk;
  private final String hostPath;
  private final PartDaemonAddress address;

  public ZkHostConfig(ZooKeeper zk, String hostPath) {
    this.zk = zk;
    this.hostPath = hostPath;

    String[] toks = hostPath.split("/");
    this.address = PartDaemonAddress.parse(toks[toks.length - 1]);
  }

  @Override
  public PartDaemonAddress getAddress() {
    return address;
  }

  @Override
  public HostState getHostState() throws IOException {
    if (getUpdateDaemonState() != UpdateDaemonState.IDLE) {
      return HostState.UPDATING;
    }
    if (getPartDaemonState() != PartDaemonState.IDLE) {
      return HostState.SERVING;
    }
    return HostState.IDLE;
  }

  @Override
  public PartDaemonState getPartDaemonState() throws IOException {
    try {
      return PartDaemonState.valueOf(new String(zk.getData(hostPath + "/part_daemon/status", false, null)));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public UpdateDaemonState getUpdateDaemonState() throws IOException {
    try {
      return UpdateDaemonState.valueOf(new String(zk.getData(hostPath + "/update_daemon/status", false, null)));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setPartDaemonState(PartDaemonState state) throws IOException {
    try {
      zk.setData(hostPath + "/part_daemon/status", state.toString().getBytes(), -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setStateChangeListener(final HostStateChangeListener listener) throws IOException {
    try {
      new StateChangeWatcher(listener).setWatch();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setUpdateDaemonState(UpdateDaemonState state) throws IOException {
    try {
      zk.setData(hostPath + "/update_daemon/status", state.toString().getBytes(), -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static ZkHostConfig create(ZooKeeper zk, String root, PartDaemonAddress partDaemonAddress) throws KeeperException, InterruptedException {
    String hostPath = root + "/" + partDaemonAddress.toString();
    zk.create(hostPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + "/parts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + "/part_daemon", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + "/part_daemon/status", PartDaemonState.IDLE.toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + "/update_daemon", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + "/update_daemon/status", UpdateDaemonState.IDLE.toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + "/.complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new ZkHostConfig(zk, hostPath);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((address == null) ? 0 : address.hashCode());
    result = prime * result + ((hostPath == null) ? 0 : hostPath.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ZkHostConfig other = (ZkHostConfig) obj;
    if (address == null) {
      if (other.address != null)
        return false;
    } else if (!address.equals(other.address))
      return false;
    if (hostPath == null) {
      if (other.hostPath != null)
        return false;
    } else if (!hostPath.equals(other.hostPath))
      return false;
    return true;
  }

  @Override
  public Set<HostDomainConfig> getAssignedDomains() throws IOException {
    List<String> domains;
    try {
      domains = zk.getChildren(hostPath + "/parts", false);
    } catch (Exception e) {
      throw new IOException(e);
    }
    Set<HostDomainConfig> results = new HashSet<HostDomainConfig>();
    for (String domain : domains) {
      results.add(new ZkHostDomainConfig(zk, hostPath + "/parts", Byte.parseByte(domain)));
    }
    return results;
  }

  @Override
  public HostDomainConfig addDomain(byte domainId) throws IOException {
    // TODO: check if we already have a domain with that id
    HostDomainConfig hdc = ZkHostDomainConfig.create(zk, hostPath + "/parts", domainId);
    return hdc;
  }

  @Override
  public HostDomainConfig getDomainById(int domainId) {
    // TODO: this should be done with a map and caching
    try {
      for (HostDomainConfig hdc : getAssignedDomains()) {
        if (hdc.getDomainId() == domainId) {
          return hdc;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
