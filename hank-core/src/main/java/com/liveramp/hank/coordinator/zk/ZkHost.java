/**
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.coordinator.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import com.liveramp.hank.coordinator.AbstractHost;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.DataLocationChangeListener;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.HostCommandQueueChangeListener;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.generated.HostAssignmentsMetadata;
import com.liveramp.hank.generated.HostDomainMetadata;
import com.liveramp.hank.generated.HostDomainPartitionMetadata;
import com.liveramp.hank.generated.HostMetadata;
import com.liveramp.hank.generated.RuntimeStatisticsSummary;
import com.liveramp.hank.generated.StatisticsMetadata;
import com.liveramp.hank.zookeeper.WatchedEnum;
import com.liveramp.hank.zookeeper.WatchedNodeListener;
import com.liveramp.hank.zookeeper.WatchedThriftNode;
import com.liveramp.hank.zookeeper.ZkPath;
import com.liveramp.hank.zookeeper.ZooKeeperPlus;

public class ZkHost extends AbstractHost {

  private static final Logger LOG = LoggerFactory.getLogger(ZkHost.class);

  private static final String STATE_PATH = "s";
  private static final String ASSIGNMENTS_PATH = "a";
  private static final String STATISTICS_PATH = "i";
  private static final String CURRENT_COMMAND_PATH = "c";
  private static final String COMMAND_QUEUE_PATH = "q";
  private static final String RUNTIME_STATISTICS_PATH = "r";

  private final ZooKeeperPlus zk;
  private final Coordinator coordinator;
  private final String path;
  private final DataLocationChangeListener dataLocationChangeListener;

  private final WatchedThriftNode<HostMetadata> metadata;
  private final WatchedThriftNode<HostAssignmentsMetadata> assignments;
  private final WatchedThriftNode<StatisticsMetadata> statistics;
  private final WatchedEnum<HostState> state;
  private final WatchedEnum<HostCommand> currentCommand;
  private final WatchedThriftNode<RuntimeStatisticsSummary> summary;

  private final Set<HostCommandQueueChangeListener> commandQueueListeners
      = new HashSet<HostCommandQueueChangeListener>();
  private final CommandQueueWatcher commandQueueWatcher;

  public static ZkHost create(ZooKeeperPlus zk,
                              Coordinator coordinator,
                              String root,
                              PartitionServerAddress partitionServerAddress,
                              DataLocationChangeListener dataLocationChangeListener,
                              List<String> flags) throws KeeperException, InterruptedException {
    String path = ZkPath.append(root, Long.toString(Math.abs(UUID.randomUUID().getLeastSignificantBits())));
    if (LOG.isTraceEnabled()) {
      LOG.trace("Creating ZkHost " + partitionServerAddress + " at " + path);
    }
    HostMetadata initialMetadata = new HostMetadata();
    initialMetadata.set_flags(Hosts.joinHostFlags(flags));
    initialMetadata.set_host_name(partitionServerAddress.getHostName());
    initialMetadata.set_port_number(partitionServerAddress.getPortNumber());
    HostAssignmentsMetadata initialAssignments = new HostAssignmentsMetadata();
    initialAssignments.set_domains(new HashMap<Integer, HostDomainMetadata>());
    return new ZkHost(zk, coordinator, path, dataLocationChangeListener, true, initialMetadata, initialAssignments);
  }

  public ZkHost(final ZooKeeperPlus zk,
                final Coordinator coordinator,
                final String path,
                final DataLocationChangeListener dataLocationChangeListener,
                final boolean create,
                final HostMetadata initialMetadata,
                final HostAssignmentsMetadata initialAssignments) throws KeeperException, InterruptedException {
    if (coordinator == null) {
      throw new IllegalArgumentException("Cannot initialize a ZkHost with a null Coordinator.");
    }
    this.zk = zk;
    this.coordinator = coordinator;
    this.path = path;
    this.dataLocationChangeListener = dataLocationChangeListener;
    this.metadata = new WatchedThriftNode<HostMetadata>(zk, path, true, create ? CreateMode.PERSISTENT : null, initialMetadata, new HostMetadata());
    this.metadata.addListener(new HostMetadataDataLocationChangeNotifier());
    this.assignments = new WatchedThriftNode<HostAssignmentsMetadata>(zk, ZkPath.append(path, ASSIGNMENTS_PATH),
        true, create ? CreateMode.PERSISTENT : null, initialAssignments, new HostAssignmentsMetadata());
    if (create) {
      zk.create(ZkPath.append(path, CURRENT_COMMAND_PATH), null);
      zk.create(ZkPath.append(path, COMMAND_QUEUE_PATH), null);
    }
    this.state = new WatchedEnum<HostState>(HostState.class, zk, ZkPath.append(path, STATE_PATH), false);
    this.state.addListener(new HostStateDataLocationChangeNotifier());
    this.statistics = new WatchedThriftNode<StatisticsMetadata>(zk, ZkPath.append(path, STATISTICS_PATH),
        false, null, null, new StatisticsMetadata());
    commandQueueWatcher = new CommandQueueWatcher();
    currentCommand = new WatchedEnum<HostCommand>(HostCommand.class, zk,
        ZkPath.append(path, CURRENT_COMMAND_PATH), true);
    if (create) {
      zk.create(ZkPath.append(path, DotComplete.NODE_NAME), null);
    }

    this.summary = new WatchedThriftNode<>(zk, ZkPath.append(path, RUNTIME_STATISTICS_PATH),
        false, null, null, new RuntimeStatisticsSummary());
  }

  private class HostStateDataLocationChangeNotifier implements WatchedNodeListener<HostState> {

    @Override
    public void onWatchedNodeChange(HostState value) {
      if (value == HostState.SERVING) {
        fireDataLocationChangeListener();
      }
    }
  }

  private class HostMetadataDataLocationChangeNotifier implements WatchedNodeListener<HostMetadata> {

    @Override
    public void onWatchedNodeChange(HostMetadata hostMetadata) {
      fireDataLocationChangeListener();
    }
  }

  private class CommandQueueWatcher extends HankWatcher {
    protected CommandQueueWatcher() throws KeeperException, InterruptedException {
      super();
    }

    @Override
    public void realProcess(WatchedEvent event) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("{}", event);
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
      zk.getChildren(ZkPath.append(path, COMMAND_QUEUE_PATH), this);
    }

  }

  @Override
  public PartitionServerAddress getAddress() {
    HostMetadata hostMetadata = metadata.get();
    return new PartitionServerAddress(hostMetadata.get_host_name(), hostMetadata.get_port_number());
  }

  @Override
  public void setAddress(final PartitionServerAddress address) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
          currentCopy.set_host_name(address.getHostName());
          currentCopy.set_port_number(address.getPortNumber());
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public HostState getState() throws IOException {
    HostState stateValue = state.get();
    if (stateValue == null) {
      return HostState.OFFLINE;
    } else {
      return stateValue;
    }
  }

  @Override
  public void setState(HostState stateValue) throws IOException {
    try {
      if (stateValue == HostState.OFFLINE) {
        zk.deleteIfExists(state.getPath());
      } else {
        zk.setOrCreate(state.getPath(), stateValue.toString(), CreateMode.EPHEMERAL);
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
      Stat stat = zk.exists(state.getPath(), false);
      if (stat == null) {
        return null;
      } else {
        return stat.getCtime();
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<String> getFlags() throws IOException {
    String flags = metadata.get().get_flags();
    return flags == null ? null : Hosts.splitHostFlags(flags);
  }

  @Override
  public void setFlags(final List<String> flags) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
          currentCopy.set_flags(Hosts.joinHostFlags(flags));
        }
      });
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setStateChangeListener(final WatchedNodeListener<HostState> listener) throws IOException {
    state.addListener(listener);
  }

  @Override
  public void cancelStateChangeListener(final WatchedNodeListener<HostState> listener) {
    state.removeListener(listener);
  }

  @Override
  public HostCommand getCurrentCommand() throws IOException {
    return currentCommand.get();
  }

  @Override
  public void enqueueCommand(HostCommand command) throws IOException {
    try {
      zk.create(ZkPath.append(path, COMMAND_QUEUE_PATH, "command_"),
          command.toString().getBytes(), CreateMode.PERSISTENT_SEQUENTIAL);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<HostCommand> getCommandQueue() throws IOException {
    try {
      List<String> children = zk.getChildren(ZkPath.append(path, COMMAND_QUEUE_PATH), false);
      Collections.sort(children);
      List<HostCommand> queue = new ArrayList<HostCommand>();
      for (String child : children) {
        queue.add(HostCommand.valueOf(zk.getString(ZkPath.append(path, COMMAND_QUEUE_PATH, child))));
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
      List<String> children = zk.getChildren(ZkPath.append(path, COMMAND_QUEUE_PATH), false);
      Collections.sort(children);

      // if there are no children, the queue is empty.
      if (children.size() == 0) {
        currentCommand.set(null);
        return null;
      }

      // parse out the actual command
      String headOfQueuePath = ZkPath.append(path, COMMAND_QUEUE_PATH, children.get(0));
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

  @Override
  public void clearCommandQueue() throws IOException {
    try {
      List<String> children = zk.getChildren(ZkPath.append(path, COMMAND_QUEUE_PATH), false);
      for (String child : children) {
        zk.delete(ZkPath.append(path, COMMAND_QUEUE_PATH, child), 0);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Set<HostDomain> getAssignedDomains() throws IOException {
    Set<HostDomain> result = new HashSet<HostDomain>();
    for (Integer domainId : assignments.get().get_domains().keySet()) {
      result.add(new ZkHostDomain(this, domainId));
    }
    return result;
  }

  @Override
  public HostDomain addDomain(Domain domain) throws IOException {
    final int domainId = domain.getId();
    try {
      assignments.update(assignments.new Updater() {
        @Override
        public void updateCopy(HostAssignmentsMetadata currentCopy) {
          HostDomainMetadata result = new HostDomainMetadata();
          result.set_partitions(new HashMap<Integer, HostDomainPartitionMetadata>());
          if (!currentCopy.get_domains().containsKey(domainId)) {
            currentCopy.get_domains().put(domainId, result);
          }
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    fireDataLocationChangeListener();
    return new ZkHostDomain(this, domainId);
  }

  @Override
  public void removeDomain(Domain domain) throws IOException {
    final int domainId = domain.getId();
    try {
      assignments.update(assignments.new Updater() {
        @Override
        public void updateCopy(HostAssignmentsMetadata currentCopy) {
          currentCopy.get_domains().remove(domainId);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    fireDataLocationChangeListener();
  }

  @Override
  public void setEphemeralStatistic(final String key, final String value) throws IOException {
    try {
      statistics.ensureCreated(CreateMode.EPHEMERAL);
      statistics.update(statistics.new Updater() {
        @Override
        public void updateCopy(StatisticsMetadata currentCopy) {
          if (currentCopy.get_statistics() == null) {
            currentCopy.set_statistics(new HashMap<String, String>());
          }
          currentCopy.get_statistics().put(key, value);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String getStatistic(String key) throws IOException {
    StatisticsMetadata result = statistics.get();
    if (result == null || result.get_statistics() == null) {
      return null;
    } else {
      return result.get_statistics().get(key);
    }
  }

  @Override
  public void setRuntimeStatisticsSummary(RuntimeStatisticsSummary summary) throws IOException {
    try {
      this.summary.ensureCreated(CreateMode.EPHEMERAL);
      this.summary.set(summary);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RuntimeStatisticsSummary getRuntimeStatisticsSummary() throws IOException {
    return this.summary.get();
  }

  @Override
  public void deleteStatistic(final String key) throws IOException {
    try {
      statistics.update(statistics.new Updater() {
        @Override
        public void updateCopy(StatisticsMetadata currentCopy) {
          if (currentCopy.get_statistics() != null) {
            currentCopy.get_statistics().remove(key);
          }
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setEnvironmentFlags(Map<String, String> flags) {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
          currentCopy.set_environment_flags(flags);
        }
      });
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> getEnvironmentFlags() {
    return metadata.get().get_environment_flags();
  }

  public void delete() throws IOException {
    try {
      zk.deleteNodeRecursively(path);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public void close() {
    state.cancelWatch();
    currentCommand.cancelWatch();
    commandQueueWatcher.cancel();
  }

  protected Domain getDomain(int domainId) {
    Domain domain = coordinator.getDomainById(domainId);
    if (domain == null) {
      LOG.warn("Domain "+domainId+" not found!");
    }
    return domain;
  }

  protected Set<HostDomainPartition> getPartitions(int domainId) {
    HostDomainMetadata hostDomainMetadata = assignments.get().get_domains().get(domainId);
    if (hostDomainMetadata == null) {
      return Collections.emptySet();
    } else {
      Set<HostDomainPartition> result = new HashSet<HostDomainPartition>();
      for (Integer partitionNumber : hostDomainMetadata.get_partitions().keySet()) {
        result.add(new ZkHostDomainPartition(this, domainId, partitionNumber));
      }
      return result;
    }
  }

  protected HostDomainPartition addPartition(final int domainId,
                                             final int partNum) throws IOException {
    try {
      assignments.update(assignments.new Updater() {
        @Override
        public void updateCopy(HostAssignmentsMetadata currentCopy) {
          final HostDomainPartitionMetadata result = new HostDomainPartitionMetadata();
          result.set_deletable(false);
          if (!currentCopy.get_domains().containsKey(domainId)) {
            currentCopy.get_domains().put(domainId, new HostDomainMetadata(new HashMap<Integer, HostDomainPartitionMetadata>()));
          }
          currentCopy.get_domains().get(domainId).get_partitions().put(partNum, result);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    return new ZkHostDomainPartition(this, domainId, partNum);
  }

  protected void removePartition(final int domainId,
                                 final int partNum) throws IOException {
    try {
      assignments.update(assignments.new Updater() {
        @Override
        public void updateCopy(HostAssignmentsMetadata currentCopy) {
          HostDomainMetadata hostDomainMetadata = currentCopy.get_domains().get(domainId);
          if (hostDomainMetadata != null) {
            hostDomainMetadata.get_partitions().remove(partNum);
          }
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  protected Integer getCurrentDomainGroupVersion(int domainId, int partitionNumber) {
    HostDomainMetadata hostDomainMetadata = assignments.get().get_domains().get(domainId);
    if (hostDomainMetadata == null) {
      return null;
    } else {
      HostDomainPartitionMetadata hostPartitionMetadata = hostDomainMetadata.get_partitions().get(partitionNumber);
      if (hostPartitionMetadata == null) {
        return null;
      } else {
        if (!hostPartitionMetadata.is_set_current_version_number()) {
          return null;
        } else {
          return hostPartitionMetadata.get_current_version_number();
        }
      }
    }
  }

  protected void setCurrentDomainGroupVersion(final int domainId,
                                              final int partitionNumber,
                                              final Integer version) throws IOException {
    try {
      assignments.update(assignments.new Updater() {
        @Override
        public void updateCopy(HostAssignmentsMetadata currentCopy) {
          HostDomainMetadata hostDomainMetadata = currentCopy.get_domains().get(domainId);
          if (hostDomainMetadata != null) {
            HostDomainPartitionMetadata hostPartitionMetadata =
                hostDomainMetadata.get_partitions().get(partitionNumber);
            if (hostPartitionMetadata != null) {
              if (version == null) {
                hostPartitionMetadata.unset_current_version_number();
              } else {
                hostPartitionMetadata.set_current_version_number(version);
              }
            }
          }
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  protected boolean isDeletable(int domainId, int partitionNumber) {
    HostDomainMetadata hostDomainMetadata = assignments.get().get_domains().get(domainId);
    if (hostDomainMetadata == null) {
      return false;
    } else {
      HostDomainPartitionMetadata hostPartitionMetadata = hostDomainMetadata.get_partitions().get(partitionNumber);
      if (hostPartitionMetadata == null) {
        return false;
      } else {
        if (!hostPartitionMetadata.is_set_deletable()) {
          return false;
        } else {
          return hostPartitionMetadata.is_deletable();
        }
      }
    }
  }

  protected void setDeletable(final int domainId,
                              final int partitionNumber,
                              final boolean deletable) throws IOException {
    try {
      assignments.update(assignments.new Updater() {
        @Override
        public void updateCopy(HostAssignmentsMetadata currentCopy) {
          HostDomainMetadata hostDomainMetadata = currentCopy.get_domains().get(domainId);
          if (hostDomainMetadata != null) {
            HostDomainPartitionMetadata hostPartitionMetadata =
                hostDomainMetadata.get_partitions().get(partitionNumber);
            if (hostPartitionMetadata != null) {
              hostPartitionMetadata.set_deletable(deletable);
            }
          }
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void fireDataLocationChangeListener() {
    if (dataLocationChangeListener != null) {
      dataLocationChangeListener.onDataLocationChange();
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getAddress() == null) ? 0 : getAddress().hashCode());
    result = prime * result + ((path == null) ? 0 : path.hashCode());
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
    if (getAddress() == null) {
      if (other.getAddress() != null) {
        return false;
      }
    } else if (!getAddress().equals(other.getAddress())) {
      return false;
    }
    if (path == null) {
      if (other.path != null) {
        return false;
      }
    } else if (!path.equals(other.path)) {
      return false;
    }
    return true;
  }
}
