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
import com.rapleaf.hank.generated.HostDomainMetadata;
import com.rapleaf.hank.generated.HostDomainPartitionMetadata;
import com.rapleaf.hank.generated.HostMetadata;
import com.rapleaf.hank.generated.StatisticsMetadata;
import com.rapleaf.hank.zookeeper.*;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;

public class NewZkHost extends AbstractHost {

  private static final Logger LOG = Logger.getLogger(NewZkHost.class);

  private static final String STATE_PATH = "s";
  private static final String STATISTICS_PATH = "i";
  private static final String CURRENT_COMMAND_PATH = "current_command";
  private static final String COMMAND_QUEUE_PATH = "command_queue";

  private final ZooKeeperPlus zk;
  private final Coordinator coordinator;
  private final String path;
  private final PartitionServerAddress address;
  private final DataLocationChangeListener dataLocationChangeListener;

  private final WatchedThriftNode<HostMetadata> metadata;
  private final WatchedThriftNode<StatisticsMetadata> statistics;
  private final WatchedEnum<HostState> state;
  private final WatchedEnum<HostCommand> currentCommand;

  private final Set<HostCommandQueueChangeListener> commandQueueListeners = new HashSet<HostCommandQueueChangeListener>();
  private final CommandQueueWatcher commandQueueWatcher;

  //
  //  private final WatchedEnum<HostCommand> currentCommand;
  //
  //  private final Set<HostCommandQueueChangeListener> commandQueueListeners = new HashSet<HostCommandQueueChangeListener>();
  //  private final CommandQueueWatcher commandQueueWatcher;
  //  private final WatchedMap<ZkHostDomain> domains;
  //
  //  public static NewZkHost create(ZooKeeperPlus zk,
  //                                 Coordinator coordinator,
  //                                 String root,
  //                                 PartitionServerAddress partitionServerAddress,
  //                                 DataLocationChangeListener dataLocationChangeListener,
  //                                 List<String> flags) throws KeeperException, InterruptedException {
  //    String hostPath = ZkPath.append(root, partitionServerAddress.toString());
  //    if (LOG.isTraceEnabled()) {
  //      LOG.trace("Creating host " + hostPath);
  //    }
  //    zk.create(hostPath, null);
  //    zk.create(ZkPath.append(hostPath, PARTS_PATH_SEGMENT), null);
  //    zk.create(ZkPath.append(hostPath, CURRENT_COMMAND_PATH_SEGMENT), null);
  //    zk.create(ZkPath.append(hostPath, COMMAND_QUEUE_PATH_SEGMENT), null);
  //    zk.create(ZkPath.append(hostPath, STATISTICS_PATH_SEGMENT), null);
  //    zk.create(ZkPath.append(hostPath, FLAGS_PATH_SEGMENT), Hosts.joinHostFlags(flags).getBytes());
  //
  //    zk.create(ZkPath.append(hostPath, DotComplete.NODE_NAME), null);
  //    return new NewZkHost(zk, coordinator, hostPath, dataLocationChangeListener);
  //  }
  //
  public NewZkHost(final ZooKeeperPlus zk,
                   final Coordinator coordinator,
                   final String path,
                   final DataLocationChangeListener dataLocationChangeListener,
                   final boolean create,
                   final HostMetadata initialMetadata) throws KeeperException, InterruptedException {
    if (coordinator == null) {
      throw new IllegalArgumentException("Cannot initialize a ZkHost with a null Coordinator.");
    }
    this.zk = zk;
    this.coordinator = coordinator;
    this.path = path;
    this.address = PartitionServerAddress.parse(ZkPath.getFilename(path));
    this.dataLocationChangeListener = dataLocationChangeListener;
    this.metadata = new WatchedThriftNode<HostMetadata>(zk, path, true, create, initialMetadata, new HostMetadata());
    this.state = new WatchedEnum<HostState>(HostState.class, zk, ZkPath.append(path, STATE_PATH), false);
    this.statistics = new WatchedThriftNode<StatisticsMetadata>(zk, ZkPath.append(path, STATISTICS_PATH),
        false, false, null, new StatisticsMetadata());
    commandQueueWatcher = new CommandQueueWatcher();
    currentCommand = new WatchedEnum<HostCommand>(HostCommand.class, zk,
        ZkPath.append(path, CURRENT_COMMAND_PATH), true);
    //    domains = new WatchedMap<ZkHostDomain>(zk, ZkPath.append(hostPath, PARTS_PATH_SEGMENT),
    //        new ElementLoader<ZkHostDomain>() {
    //          @Override
    //          public ZkHostDomain load(ZooKeeperPlus zk,
    //                                   String basePath,
    //                                   String relPath) throws KeeperException, InterruptedException {
    //            if (!ZkPath.isHidden(relPath)) {
    //              Domain domain = coordinator.getDomain(relPath);
    //              if (domain == null) {
    //                throw new RuntimeException(String.format("Could not load domain %s from Coordinator.", relPath));
    //              }
    //              return new ZkHostDomain(zk, basePath, domain, dataLocationChangeListener);
    //            }
    //            return null;
    //          }
    //        });
    //    domains.addListener(new DomainsWatchedMapListener());
  }

  //
  //  private class DomainsWatchedMapListener implements WatchedMapListener<ZkHostDomain> {
  //
  //    @Override
  //    public void onWatchedMapChange(WatchedMap<ZkHostDomain> zkHostDomainWatchedMap) {
  //      fireDataLocationChangeListener();
  //    }
  //  }
  //
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
            listener.onCommandQueueChange(NewZkHost.this);
          }
      }
    }

    @Override
    public void setWatch() throws KeeperException, InterruptedException {
      zk.getChildren(ZkPath.append(path, COMMAND_QUEUE_PATH), this);
    }
  }

  //  @Override
  //  public HostDomain addDomain(Domain domain) throws IOException {
  //    if (domains.containsKey(domain.getName())) {
  //      throw new IOException("Domain " + domain + " is already assigned to this host!");
  //    }
  //    ZkHostDomain hdc = ZkHostDomain.create(zk, ZkPath.append(hostPath, PARTS_PATH_SEGMENT), domain, dataLocationChangeListener);
  //    domains.put(domain.getName(), hdc);
  //    fireDataLocationChangeListener();
  //    return hdc;
  //  }
  //
  //  @Override
  //  public boolean removeDomain(Domain domain) throws IOException {
  //    ZkHostDomain hostDomain = domains.remove(domain.getName());
  //    if (hostDomain == null) {
  //      return false;
  //    } else {
  //      hostDomain.delete();
  //      fireDataLocationChangeListener();
  //      return true;
  //    }
  //  }
  //
  //  @Override
  //  public void setCommandQueueChangeListener(HostCommandQueueChangeListener listener) {
  //    synchronized (commandQueueListeners) {
  //      commandQueueListeners.add(listener);
  //    }
  //  }
  //
  //  @Override
  //  public void setCurrentCommandChangeListener(WatchedNodeListener<HostCommand> listener) {
  //    currentCommand.addListener(listener);
  //  }
  //
  //  public void close() {
  //    hostState.cancelWatch();
  //    currentCommand.cancelWatch();
  //    commandQueueWatcher.cancel();
  //  }
  //
  //  private void fireDataLocationChangeListener() {
  //    if (dataLocationChangeListener != null) {
  //      dataLocationChangeListener.onDataLocationChange();
  //    }
  //  }

  @Override
  public PartitionServerAddress getAddress() {
    return address;
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
    return metadata.get().get_flags();
  }

  @Override
  public void setFlags(final List<String> newFlags) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
          currentCopy.set_flags(newFlags);
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
    for (Integer domainId : metadata.get().get_domains().keySet()) {
      result.add(new NewZkHostDomain(this, domainId));
    }
    return result;
  }

  @Override
  public HostDomain addDomain(Domain domain) throws IOException {
    final int domainId = domain.getId();
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
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
    return new NewZkHostDomain(this, domainId);
  }

  @Override
  public void removeDomain(Domain domain) throws IOException {
    final int domainId = domain.getId();
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
          currentCopy.get_domains().remove(domainId);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
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
    return coordinator.getDomainById(domainId);
  }

  protected Set<HostDomainPartition> getPartitions(int domainId) {
    HostDomainMetadata hostDomainMetadata = metadata.get().get_domains().get(domainId);
    if (hostDomainMetadata == null) {
      return null;
    } else {
      Set<HostDomainPartition> result = new HashSet<HostDomainPartition>();
      for (Integer partitionNumber : hostDomainMetadata.get_partitions().keySet()) {
        result.add(new NewZkHostDomainPartition(this, domainId, partitionNumber));
      }
      return result;
    }
  }

  protected HostDomainPartition addPartition(final int domainId,
                                             final int partNum) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
          final HostDomainPartitionMetadata result = new HostDomainPartitionMetadata();
          result.set_deletable(false);
          currentCopy.get_domains().get(domainId).get_partitions().put(partNum, result);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    return new NewZkHostDomainPartition(this, domainId, partNum);
  }

  protected void removePartition(final int domainId,
                                 final int partNum) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
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
    HostDomainMetadata hostDomainMetadata = metadata.get().get_domains().get(domainId);
    if (hostDomainMetadata == null) {
      return null;
    } else {
      HostDomainPartitionMetadata hostPartitionMetadata = hostDomainMetadata.get_partitions().get(partitionNumber);
      if (hostPartitionMetadata == null) {
        return null;
      } else {
        return hostPartitionMetadata.get_current_version_number();
      }
    }
  }

  protected void setCurrentDomainGroupVersion(final int domainId,
                                              final int partitionNumber,
                                              final Integer version) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
          HostDomainMetadata hostDomainMetadata = currentCopy.get_domains().get(domainId);
          if (hostDomainMetadata != null) {
            HostDomainPartitionMetadata hostPartitionMetadata = hostDomainMetadata.get_partitions().get(partitionNumber);
            if (hostPartitionMetadata != null) {
              hostPartitionMetadata.set_current_version_number(version);
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
    HostDomainMetadata hostDomainMetadata = metadata.get().get_domains().get(domainId);
    if (hostDomainMetadata == null) {
      return false;
    } else {
      HostDomainPartitionMetadata hostPartitionMetadata = hostDomainMetadata.get_partitions().get(partitionNumber);
      if (hostPartitionMetadata == null) {
        return false;
      } else {
        return hostPartitionMetadata.is_deletable();
      }
    }
  }

  protected void setDeletable(final int domainId,
                              final int partitionNumber,
                              final boolean deletable) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(HostMetadata currentCopy) {
          HostDomainMetadata hostDomainMetadata = currentCopy.get_domains().get(domainId);
          if (hostDomainMetadata != null) {
            HostDomainPartitionMetadata hostPartitionMetadata = hostDomainMetadata.get_partitions().get(partitionNumber);
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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((address == null) ? 0 : address.hashCode());
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
    NewZkHost other = (NewZkHost) obj;
    if (address == null) {
      if (other.address != null) {
        return false;
      }
    } else if (!address.equals(other.address)) {
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
