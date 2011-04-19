/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostCommandQueueChangeListener;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.HostStateChangeListener;
import com.rapleaf.hank.coordinator.PartDaemonAddress;

public class ZkHostConfig extends BaseZkConsumer implements HostConfig {
  private static final Logger LOG = Logger.getLogger(ZkHostConfig.class);

  private static final String STATUS_PATH_SEGMENT = "/status";
  private static final String COMPLETE_PATH_SEGMENT = "/.complete";
  private static final String PARTS_PATH_SEGMENT = "/parts";
  private static final String COMMAND_QUEUE_PATH_SEGMENT = "/command_queue";
  private static final String CURRENT_COMMAND_PATH_SEGMENT = "/current_command";

  private class CommandQueueWatcher implements Watcher {
    private final HostCommandQueueChangeListener listener;

    public CommandQueueWatcher(HostCommandQueueChangeListener listener) throws KeeperException, InterruptedException {
      this.listener = listener;
      setWatch();
    }

    public void process(WatchedEvent event) {
      LOG.trace(event);
      // connect/disconnect message
      if (event.getType() == EventType.None) {
        return;
      }
      switch (event.getType()) {
        case NodeCreated:
        case NodeDeleted:
        case NodeDataChanged:
        case NodeChildrenChanged:
          listener.onCommandQueueChange(ZkHostConfig.this);
          // reset callback
          try {
            setWatch();
          } catch (Exception e) {
            LOG.error("Failed to reset watch!", e);
          }
      }
    }

    private void setWatch() throws KeeperException, InterruptedException {
      zk.getChildren(hostPath + COMMAND_QUEUE_PATH_SEGMENT, this);
    }
  }

  private class StateChangeWatcher implements Watcher {
    private boolean cancelled = false;

    public void process(WatchedEvent event) {
      switch (event.getState()) {
        case Disconnected:
        case Expired:
          return;
      }
      switch (event.getType()) {
        case NodeCreated:
        case NodeDeleted:
        case NodeDataChanged:
          for (HostStateChangeListener listener : stateListeners) {
            listener.onHostStateChange(ZkHostConfig.this);
          }
          if (!cancelled) {
            // reset callback
            try {
              setWatch();
            } catch (Exception e) {
              LOG.error("Failed to reset watch!", e);
            }
          }
      }
    }

    private void setWatch() throws KeeperException, InterruptedException {
      if (zk.exists(hostPath + STATUS_PATH_SEGMENT, this) != null) {
        zk.getData(hostPath + STATUS_PATH_SEGMENT, this, new Stat());
      }
    }

    public void cancel() {
      cancelled = true;
    }
  }

  private final ZooKeeper zk;
  private final String hostPath;
  private final PartDaemonAddress address;

  private final Set<HostStateChangeListener> stateListeners = new HashSet<HostStateChangeListener>();

  private StateChangeWatcher stateChangeWatcher;

  public ZkHostConfig(ZooKeeper zk, String hostPath) throws KeeperException, InterruptedException {
    super(zk);
    this.zk = zk;
    this.hostPath = hostPath;

    String[] toks = hostPath.split("/");
    this.address = PartDaemonAddress.parse(toks[toks.length - 1]);

    stateChangeWatcher = new StateChangeWatcher();
    stateChangeWatcher.setWatch();
  }

  @Override
  public PartDaemonAddress getAddress() {
    return address;
  }

  @Override
  public HostState getState() throws IOException {
    try {
      if (zk.exists(hostPath + STATUS_PATH_SEGMENT, false) == null) {
        return HostState.OFFLINE;
      }
      try {
        return HostState.valueOf(getString(hostPath + STATUS_PATH_SEGMENT));
      } catch (KeeperException e) {
        if (e.code() == Code.NONODE) {
          // the node disappeared between our exists check and our get. must be
          // offline now.
          return HostState.OFFLINE;
        }
        throw (e);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setStateChangeListener(final HostStateChangeListener listener) throws IOException {
    synchronized (stateListeners) {
      stateListeners.add(listener);
    }
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
      domains = zk.getChildren(hostPath + PARTS_PATH_SEGMENT, false);
    } catch (Exception e) {
      throw new IOException(e);
    }
    Set<HostDomainConfig> results = new HashSet<HostDomainConfig>();
    for (String domain : domains) {
      results.add(new ZkHostDomainConfig(zk, hostPath + PARTS_PATH_SEGMENT, Integer.parseInt(domain)));
    }
    return results;
  }

  @Override
  public HostDomainConfig addDomain(int domainId) throws IOException {
    try {
      if (zk.exists(hostPath + PARTS_PATH_SEGMENT + "/" + domainId, false) != null) {
        throw new IOException("Domain " + domainId + " is already assigned to this host!");
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    HostDomainConfig hdc = ZkHostDomainConfig.create(zk, hostPath + PARTS_PATH_SEGMENT, domainId);
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
  public HostCommand getCurrentCommand() throws IOException {
    try {
      String commandString = getString(hostPath + CURRENT_COMMAND_PATH_SEGMENT);
      if (commandString == null) {
        return null;
      }
      return HostCommand.valueOf(commandString);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isOnline() throws IOException {
    return getState() != HostState.OFFLINE;
  }

  @Override
  public void setState(HostState state) throws IOException {
    try {
      if (state == HostState.OFFLINE) {
        deleteIfExists(hostPath + STATUS_PATH_SEGMENT);
      } else {
        setOrCreate(hostPath + STATUS_PATH_SEGMENT, state.toString(), CreateMode.EPHEMERAL);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void completeCommand() throws IOException {
    try {
      zk.setData(hostPath + CURRENT_COMMAND_PATH_SEGMENT, null, -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void enqueueCommand(HostCommand command) throws IOException {
    try {
      zk.create(hostPath + COMMAND_QUEUE_PATH_SEGMENT + "/command_",
          command.toString().getBytes(),
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT_SEQUENTIAL);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<HostCommand> getCommandQueue() throws IOException {
    try {
      List<String> children = zk.getChildren(hostPath + COMMAND_QUEUE_PATH_SEGMENT, false);
      Collections.sort(children);
      List<HostCommand> queue = new ArrayList<HostCommand>();
      for (String child : children) {
        queue.add(HostCommand.valueOf(getString(hostPath + COMMAND_QUEUE_PATH_SEGMENT + "/" + child)));
      }
      return queue;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public HostCommand processNextCommand() throws IOException {
    try {
      // get the queue and sort so we have correct ordering
      List<String> children = zk.getChildren(hostPath + COMMAND_QUEUE_PATH_SEGMENT, false);
      Collections.sort(children);

      // if there are no children, the queue is empty.
      if (children.size() == 0) {
        return null;
      }

      // parse out the actual command
      String headOfQueuePath = hostPath + COMMAND_QUEUE_PATH_SEGMENT + "/" + children.get(0);
      HostCommand nextCommand = HostCommand.valueOf(getString(headOfQueuePath));

      // set the current command
      zk.setData(hostPath + CURRENT_COMMAND_PATH_SEGMENT, nextCommand.toString().getBytes(), -1);
      // delete the head of the queue
      zk.delete(headOfQueuePath, -1);

      return nextCommand;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setCommandQueueChangeListener(HostCommandQueueChangeListener listener)
  throws IOException {
    try {
      new CommandQueueWatcher(listener);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static ZkHostConfig create(ZooKeeper zk, String root, PartDaemonAddress partDaemonAddress) throws KeeperException, InterruptedException {
    String hostPath = root + "/" + partDaemonAddress.toString();
    LOG.trace("creating host " + hostPath);
    zk.create(hostPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + PARTS_PATH_SEGMENT, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + CURRENT_COMMAND_PATH_SEGMENT, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + COMMAND_QUEUE_PATH_SEGMENT, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(hostPath + COMPLETE_PATH_SEGMENT, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new ZkHostConfig(zk, hostPath);
  }

  @Override
  public String toString() {
    return "ZkHostConfig [address=" + address + "]";
  }

  @Override
  public void cancelStateChangeListener(HostStateChangeListener listener) {
    synchronized (stateListeners) {
      stateListeners.remove(listener);
    }
  }

  public void close() {
    stateChangeWatcher.cancel();
  }
}
