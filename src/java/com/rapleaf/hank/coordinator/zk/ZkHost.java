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

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;

public class ZkHost extends AbstractHost {
  private static final Logger LOG = Logger.getLogger(ZkHost.class);

  private static final String STATUS_PATH_SEGMENT = "status";
  private static final String PARTS_PATH_SEGMENT = "parts";
  private static final String COMMAND_QUEUE_PATH_SEGMENT = "command_queue";
  private static final String CURRENT_COMMAND_PATH_SEGMENT = "current_command";

  private class CommandQueueWatcher extends HankWatcher {
    protected CommandQueueWatcher() throws KeeperException, InterruptedException {
      super();
    }

    @Override
    public void realProcess(WatchedEvent event) {
      LOG.trace(event);
      switch (event.getType()) {
        case NodeCreated:
        case NodeDeleted:
        case NodeDataChanged:
        case NodeChildrenChanged:
          for (HostCommandQueueChangeListener listener : commandQueueListeners) {
            listener.onCommandQueueChange(ZkHost.this);
          }
      }
    }

    @Override
    public void setWatch() throws KeeperException, InterruptedException {
      zk.getChildren(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT), this);
    }
  }

  private class StateChangeWatcher extends HankWatcher {
    public StateChangeWatcher() throws KeeperException, InterruptedException {
      super();
    }

    @Override
    public void realProcess(WatchedEvent event) {
      switch (event.getType()) {
        case NodeCreated:
        case NodeDeleted:
        case NodeDataChanged:
          for (HostStateChangeListener listener : stateListeners) {
            listener.onHostStateChange(ZkHost.this);
          }
      }
    }

    @Override
    public void setWatch() throws KeeperException, InterruptedException {
      if (zk.exists(ZkPath.append(hostPath, STATUS_PATH_SEGMENT), this) != null) {
        zk.getData(ZkPath.append(hostPath, STATUS_PATH_SEGMENT), this, new Stat());
      }
    }
  }

  public static ZkHost create(ZooKeeperPlus zk,
                              Coordinator coordinator,
                              String root,
                              PartitionServerAddress partitionServerAddress) throws KeeperException, InterruptedException {
    String hostPath = ZkPath.append(root, partitionServerAddress.toString());
    LOG.trace("creating host " + hostPath);
    zk.create(hostPath, null);
    zk.create(ZkPath.append(hostPath, PARTS_PATH_SEGMENT), null);
    zk.create(ZkPath.append(hostPath, CURRENT_COMMAND_PATH_SEGMENT), null);
    zk.create(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT), null);
    zk.create(ZkPath.append(hostPath, DotComplete.NODE_NAME), null);
    return new ZkHost(zk, coordinator, hostPath);
  }

  private final ZooKeeperPlus zk;
  private final String hostPath;
  private final PartitionServerAddress address;

  private final Set<HostStateChangeListener> stateListeners = new HashSet<HostStateChangeListener>();
  private final StateChangeWatcher stateChangeWatcher;

  private final Set<HostCommandQueueChangeListener> commandQueueListeners = new HashSet<HostCommandQueueChangeListener>();
  private final CommandQueueWatcher commandQueueWatcher;
  private final WatchedMap<ZkHostDomain> domains;

  public ZkHost(ZooKeeperPlus zk, final Coordinator coordinator, String hostPath) throws KeeperException, InterruptedException {
    this.zk = zk;
    this.hostPath = hostPath;
    this.address = PartitionServerAddress.parse(ZkPath.getFilename(hostPath));

    stateChangeWatcher = new StateChangeWatcher();
    stateChangeWatcher.setWatch();
    commandQueueWatcher = new CommandQueueWatcher();
    domains = new WatchedMap<ZkHostDomain>(zk, ZkPath.append(hostPath, PARTS_PATH_SEGMENT), new ElementLoader<ZkHostDomain>() {
      @Override
      public ZkHostDomain load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        if (!ZkPath.isHidden(relPath)) {
          Domain domain = coordinator.getDomain(relPath);
          if (domain == null) {
            throw new RuntimeException(String.format("Could not load domain %s from Coordinator.", relPath));
          }
          return new ZkHostDomain(zk, basePath, domain);
        }
        return null;
      }
    }/*, new DotComplete()*/);
  }

  @Override
  public PartitionServerAddress getAddress() {
    return address;
  }

  @Override
  public HostState getState() throws IOException {
    try {
      if (zk.exists(ZkPath.append(hostPath, STATUS_PATH_SEGMENT), false) == null) {
        return HostState.OFFLINE;
      }
      try {
        return HostState.valueOf(zk.getString(ZkPath.append(hostPath, STATUS_PATH_SEGMENT)));
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
    ZkHost other = (ZkHost) obj;
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
  public Set<HostDomain> getAssignedDomains() throws IOException {
    return new HashSet<HostDomain>(domains.values());
  }

  @Override
  public HostDomain addDomain(Domain domain) throws IOException {
    try {
      if (domains.containsKey(domain.getName())) {
        throw new IOException("Domain " + domain + " is already assigned to this host!");
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    ZkHostDomain hdc = ZkHostDomain.create(zk, ZkPath.append(hostPath, PARTS_PATH_SEGMENT), domain);
    domains.put(domain.getName(), hdc);
    return hdc;
  }

  @Override
  public HostCommand getCurrentCommand() throws IOException {
    try {
      String commandString = zk.getString(ZkPath.append(hostPath, CURRENT_COMMAND_PATH_SEGMENT));
      if (commandString == null) {
        return null;
      }
      return HostCommand.valueOf(commandString);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setState(HostState state) throws IOException {
    try {
      if (state == HostState.OFFLINE) {
        zk.deleteIfExists(ZkPath.append(hostPath, STATUS_PATH_SEGMENT));
      } else {
        zk.setOrCreate(ZkPath.append(hostPath, STATUS_PATH_SEGMENT), state.toString(), CreateMode.EPHEMERAL);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void completeCommand() throws IOException {
    try {
      zk.setData(ZkPath.append(hostPath, CURRENT_COMMAND_PATH_SEGMENT), null, -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void enqueueCommand(HostCommand command) throws IOException {
    try {
      zk.create(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT, "command_"),
          command.toString().getBytes(), CreateMode.PERSISTENT_SEQUENTIAL);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<HostCommand> getCommandQueue() throws IOException {
    try {
      List<String> children = zk.getChildren(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT), false);
      Collections.sort(children);
      List<HostCommand> queue = new ArrayList<HostCommand>();
      for (String child : children) {
        queue.add(HostCommand.valueOf(zk.getString(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT, child))));
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
      List<String> children = zk.getChildren(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT), false);
      Collections.sort(children);

      // if there are no children, the queue is empty.
      if (children.size() == 0) {
        zk.setData(ZkPath.append(hostPath, CURRENT_COMMAND_PATH_SEGMENT), null, -1);
        return null;
      }

      // parse out the actual command
      String headOfQueuePath = ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT, children.get(0));
      HostCommand nextCommand = HostCommand.valueOf(zk.getString(headOfQueuePath));

      // set the current command
      zk.setData(ZkPath.append(hostPath, CURRENT_COMMAND_PATH_SEGMENT), nextCommand.toString().getBytes(), -1);
      // delete the head of the queue
      zk.delete(headOfQueuePath, -1);

      return nextCommand;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setCommandQueueChangeListener(HostCommandQueueChangeListener listener) {
    synchronized (commandQueueListeners) {
      commandQueueListeners.add(listener);
    }
  }

  @Override
  public void cancelStateChangeListener(HostStateChangeListener listener) {
    synchronized (stateListeners) {
      stateListeners.remove(listener);
    }
  }

  public void close() {
    stateChangeWatcher.cancel();
    commandQueueWatcher.cancel();
  }

  @Override
  public void clearCommandQueue() throws IOException {
    try {
      List<String> children = zk.getChildren(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT), false);
      for (String child : children) {
        zk.delete(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT, child), 0);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
