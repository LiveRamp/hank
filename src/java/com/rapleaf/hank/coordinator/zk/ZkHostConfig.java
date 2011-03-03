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

import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartDaemonAddress;

public class ZkHostConfig extends BaseZkConsumer implements HostConfig {
  private class StateChangeWatcher implements Watcher {
    private final HostStateChangeListener listener;

    public StateChangeWatcher(HostStateChangeListener l) {
      listener = l;
    }

    public void process(WatchedEvent event) {
      switch (event.getType()) {
        case NodeCreated:
        case NodeDeleted:
        case NodeDataChanged:
          listener.onHostStateChange(ZkHostConfig.this);
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
      if (zk.exists(hostPath + "/status", this) != null) {
        zk.getData(hostPath + "/status", this, new Stat());
      }
      zk.getData(hostPath + "/command", this, new Stat());
    }
  }

  private final ZooKeeper zk;
  private final String hostPath;
  private final PartDaemonAddress address;

  public ZkHostConfig(ZooKeeper zk, String hostPath) {
    super(zk);
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
  public HostState getState() throws IOException {
    try {
      if (zk.exists(hostPath + "/status", false) == null) {
        return HostState.OFFLINE;
      }
      return HostState.valueOf(getString(hostPath + "/status"));
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

  public static ZkHostConfig create(ZooKeeper zk, String root, PartDaemonAddress partDaemonAddress) throws KeeperException, InterruptedException {
    String hostPath = root + "/" + partDaemonAddress.toString();
    zk.create(hostPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + "/parts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + "/command", HostCommand.GO_TO_IDLE.toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
  public HostDomainConfig addDomain(int domainId) throws IOException {
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

  @Override
  public HostCommand getCommand() throws IOException {
    try {
      return HostCommand.valueOf(getString(hostPath + "/command"));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isOnline() throws IOException {
    return getState() != HostState.OFFLINE;
  }

  @Override
  public void setCommand(HostCommand command) throws IOException {
    try {
      zk.setData(hostPath + "/command", command.toString().getBytes(), -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setState(HostState state) throws IOException {
    try {
      if (state == HostState.OFFLINE) {
        if (zk.exists(hostPath + "/status", false) == null) {
          // already offline
        } else {
          zk.delete(hostPath + "/status", -1);
        }
      } else {
        if (zk.exists(hostPath + "/status", false) == null) {
          zk.create(hostPath + "/status", state.toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
          zk.setData(hostPath + "/status", state.toString().getBytes(), -1);
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
