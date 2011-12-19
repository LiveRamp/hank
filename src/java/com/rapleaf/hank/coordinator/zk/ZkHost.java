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
import com.rapleaf.hank.zookeeper.*;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import java.io.IOException;
import java.util.*;

public class ZkHost extends AbstractHost {
  private static final Logger LOG = Logger.getLogger(ZkHost.class);

  private static final String STATUS_PATH_SEGMENT = "status";
  private static final String PARTS_PATH_SEGMENT = "parts";
  private static final String COMMAND_QUEUE_PATH_SEGMENT = "command_queue";
  private static final String CURRENT_COMMAND_PATH_SEGMENT = "current_command";
  private static final String STATISTICS_PATH_SEGMENT = "statistics";

  private final ZooKeeperPlus zk;
  private final String hostPath;
  private final PartitionServerAddress address;

  private final WatchedEnum<HostState> hostState;
  private final WatchedEnum<HostCommand> currentCommand;

  private final Set<HostCommandQueueChangeListener> commandQueueListeners = new HashSet<HostCommandQueueChangeListener>();
  private final CommandQueueWatcher commandQueueWatcher;
  private final WatchedMap<ZkHostDomain> domains;
  private final WatchedMap<WatchedString> statistics;

  public static ZkHost create(ZooKeeperPlus zk,
                              Coordinator coordinator,
                              String root,
                              PartitionServerAddress partitionServerAddress) throws KeeperException, InterruptedException {
    String hostPath = ZkPath.append(root, partitionServerAddress.toString());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Creating host " + hostPath);
    }
    zk.create(hostPath, null);
    zk.create(ZkPath.append(hostPath, PARTS_PATH_SEGMENT), null);
    zk.create(ZkPath.append(hostPath, CURRENT_COMMAND_PATH_SEGMENT), null);
    zk.create(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT), null);
    zk.create(ZkPath.append(hostPath, STATISTICS_PATH_SEGMENT), null);

    zk.create(ZkPath.append(hostPath, DotComplete.NODE_NAME), null);
    return new ZkHost(zk, coordinator, hostPath);
  }

  public ZkHost(ZooKeeperPlus zk, final Coordinator coordinator, String hostPath) throws KeeperException, InterruptedException {
    this.zk = zk;
    this.hostPath = hostPath;
    this.address = PartitionServerAddress.parse(ZkPath.getFilename(hostPath));

    if (coordinator == null) {
      throw new IllegalArgumentException("Cannot initialize a ZkHost with a null Coordinator.");
    }

    hostState = new WatchedEnum<HostState>(HostState.class,
        zk, ZkPath.append(hostPath, STATUS_PATH_SEGMENT), false);
    commandQueueWatcher = new CommandQueueWatcher();
    currentCommand = new WatchedEnum<HostCommand>(HostCommand.class, zk,
        ZkPath.append(hostPath, CURRENT_COMMAND_PATH_SEGMENT), true);
    domains = new WatchedMap<ZkHostDomain>(zk, ZkPath.append(hostPath, PARTS_PATH_SEGMENT),
        new ElementLoader<ZkHostDomain>() {
          @Override
          public ZkHostDomain load(ZooKeeperPlus zk,
                                   String basePath,
                                   String relPath) throws KeeperException, InterruptedException {
            if (!ZkPath.isHidden(relPath)) {
              Domain domain = coordinator.getDomain(relPath);
              if (domain == null) {
                throw new RuntimeException(String.format("Could not load domain %s from Coordinator.", relPath));
              }
              return new ZkHostDomain(zk, basePath, domain);
            }
            return null;
          }
        });

    try {
      // TODO: remove migration code
      zk.create(ZkPath.append(hostPath, STATISTICS_PATH_SEGMENT), null);
    } catch (Throwable t) {
    }

    statistics = new WatchedMap<WatchedString>(zk, ZkPath.append(hostPath, STATISTICS_PATH_SEGMENT),
        new WatchedMap.ElementLoader<WatchedString>() {
          @Override
          public WatchedString load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
            if (!ZkPath.isHidden(relPath)) {
              return new WatchedString(zk, ZkPath.append(basePath, relPath), true);
            } else {
              return null;
            }
          }
        });
  }

  private class CommandQueueWatcher extends HankWatcher {
    protected CommandQueueWatcher() throws KeeperException, InterruptedException {
      super();
    }

    @Override
    public void realProcess(WatchedEvent event) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(event);
      }
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

  @Override
  public PartitionServerAddress getAddress() {
    return address;
  }

  @Override
  public HostState getState() throws IOException {
    HostState state = hostState.get();
    if (state == null) {
      return HostState.OFFLINE;
    } else {
      return state;
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
  public void setStateChangeListener(final WatchedNodeListener<HostState> listener) throws IOException {
    hostState.addListener(listener);
  }

  @Override
  public void cancelStateChangeListener(final WatchedNodeListener<HostState> listener) {
    hostState.removeListener(listener);
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
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ZkHost other = (ZkHost) obj;
    if (address == null) {
      if (other.address != null) {
        return false;
      }
    } else if (!address.equals(other.address)) {
      return false;
    }
    if (hostPath == null) {
      if (other.hostPath != null) {
        return false;
      }
    } else if (!hostPath.equals(other.hostPath)) {
      return false;
    }
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
    return currentCommand.get();
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
  public HostCommand nextCommand() throws IOException {
    try {
      // get the queue and sort so we have correct ordering
      List<String> children = zk.getChildren(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT), false);
      Collections.sort(children);

      // if there are no children, the queue is empty.
      if (children.size() == 0) {
        currentCommand.set(null);
        return null;
      }

      // parse out the actual command
      String headOfQueuePath = ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT, children.get(0));
      HostCommand nextCommand = HostCommand.valueOf(zk.getString(headOfQueuePath));

      // set the current command first (modifying the queue will call the queue listeners)
      currentCommand.set(nextCommand);
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
  public void setCurrentCommandChangeListener(WatchedNodeListener<HostCommand> listener) {
    currentCommand.addListener(listener);
  }

  public void close() {
    hostState.cancelWatch();
    currentCommand.cancelWatch();
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

  @Override
  public Long getUpSince() throws IOException {
    if (getState() == HostState.OFFLINE) {
      return null;
    }

    try {
      return zk.exists(ZkPath.append(hostPath, STATUS_PATH_SEGMENT), false).getCtime();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setEphemeralStatistic(String key, String value) throws IOException {
    String path = ZkPath.append(hostPath, STATISTICS_PATH_SEGMENT, key);
    try {
      zk.setOrCreate(path, value, CreateMode.EPHEMERAL);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public String getStatistic(String key) throws IOException {
    WatchedString statistic = statistics.get(key);
    if (statistic != null) {
      return statistic.get();
    } else {
      return null;
    }
  }

  @Override
  public void deleteStatistic(String key) throws IOException {
    String path = ZkPath.append(hostPath, STATISTICS_PATH_SEGMENT, key);
    try {
      zk.deleteIfExists(path);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
