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
package com.liveramp.hank.test.coordinator;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.liveramp.hank.coordinator.AbstractHost;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.HostCommandQueueChangeListener;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.generated.RuntimeStatisticsSummary;
import com.liveramp.hank.zookeeper.WatchedNodeListener;

public class MockHost extends AbstractHost {

  private PartitionServerAddress address;
  private HostState state = HostState.OFFLINE;
  private List<HostCommand> commandQueue = new LinkedList<HostCommand>();
  private HostCommand currentCommand;
  private HostCommand lastEnqueuedCommand;
  private final Set<HostCommandQueueChangeListener> commandQueueChangeListeners = new HashSet<HostCommandQueueChangeListener>();
  private final Set<WatchedNodeListener<HostCommand>> currentCommandChangeListeners
      = new HashSet<WatchedNodeListener<HostCommand>>();
  private final Set<WatchedNodeListener<HostState>> hostStateChangeListeners
      = new HashSet<WatchedNodeListener<HostState>>();
  private final Set<HostDomain> hostDomains = new HashSet<HostDomain>();
  private Map<String, String> statistics = new HashMap<String, String>();
  private final Set<Domain> removedDomains = new HashSet<Domain>();
  private Map<String, String> environmentFlags = new HashMap<>();
  private RuntimeStatisticsSummary runtimeStatisticsSummary;

  public MockHost(PartitionServerAddress address) {
    this.address = address;
  }

  @Override
  public HostDomain addDomain(Domain domain) throws IOException {
    HostDomain hostDomain = new MockHostDomain(domain);
    hostDomains.add(hostDomain);
    return hostDomain;
  }

  @Override
  public void removeDomain(Domain domain) throws IOException {
    removedDomains.add(domain);
    HostDomain hostDomain = getHostDomain(domain);
    if (hostDomain != null) {
      hostDomains.remove(hostDomain);
    }
  }

  public boolean isRemoved(Domain domain) {
    return removedDomains.contains(domain);
  }

  public MockHostDomain addMockDomain(Domain domain,
                                      int... partitionNumberAndVersionPairs) throws IOException {
    MockHostDomain hostDomain = new MockHostDomain(domain, partitionNumberAndVersionPairs);
    hostDomains.add(hostDomain);
    return hostDomain;
  }

  @Override
  public PartitionServerAddress getAddress() {
    return address;
  }

  @Override
  public void setAddress(PartitionServerAddress address) throws IOException {
    this.address = address;
  }

  @Override
  public Set<HostDomain> getAssignedDomains() throws IOException {
    return hostDomains;
  }

  @Override
  public HostState getState() throws IOException {
    return state;
  }

  @Override
  public void setStateChangeListener(WatchedNodeListener<HostState> listener) {
    hostStateChangeListeners.add(listener);
  }

  @Override
  public HostCommand getCurrentCommand() throws IOException {
    return currentCommand;
  }

  public void setCurrentCommand(HostCommand command) {
    currentCommand = command;
    notifyCurrentCommandChangeListeners();
  }

  @Override
  public void setState(HostState state) throws IOException {
    this.state = state;
    notifyHostStateChangeListeners();
  }

  private void notifyHostStateChangeListeners() {
    for (WatchedNodeListener<HostState> listener : hostStateChangeListeners) {
      listener.onWatchedNodeChange(state);
    }
  }

  @Override
  public void enqueueCommand(HostCommand command) throws IOException {
    commandQueue.add(command);
    lastEnqueuedCommand = command;
    notifyCommandQueueChangeListeners();
  }

  @Override
  public List<HostCommand> getCommandQueue() throws IOException {
    return commandQueue;
  }

  @Override
  public HostCommand nextCommand() throws IOException {
    if (commandQueue.size() > 0) {
      setCurrentCommand(commandQueue.remove(0));
      notifyCommandQueueChangeListeners();
    } else {
      setCurrentCommand(null);
    }
    return currentCommand;
  }

  @Override
  public void setCommandQueueChangeListener(HostCommandQueueChangeListener listener) throws IOException {
    commandQueueChangeListeners.add(listener);
  }

  protected void notifyCommandQueueChangeListeners() {
    for (HostCommandQueueChangeListener listener : commandQueueChangeListeners) {
      listener.onCommandQueueChange(this);
    }
  }

  @Override
  public void setCurrentCommandChangeListener(WatchedNodeListener<HostCommand> listener) throws IOException {
    currentCommandChangeListeners.add(listener);
  }

  protected void notifyCurrentCommandChangeListeners() {
    for (WatchedNodeListener<HostCommand> listener : currentCommandChangeListeners) {
      listener.onWatchedNodeChange(currentCommand);
    }
  }

  @Override
  public void cancelStateChangeListener(WatchedNodeListener<HostState> listener) {
  }

  @Override
  public void clearCommandQueue() throws IOException {
    commandQueue.clear();
    notifyCommandQueueChangeListeners();
  }

  public HostCommand getAndClearLastEnqueuedCommand() {
    HostCommand result = lastEnqueuedCommand;
    lastEnqueuedCommand = null;
    return result;
  }

  @Override
  public Long getUpSince() throws IOException {
    return 0L;
  }

  @Override
  public List<String> getFlags() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public void setFlags(List<String> flags) throws IOException {
  }

  @Override
  public void setEphemeralStatistic(String key, String value) throws IOException {
    statistics.put(key, value);
  }

  @Override
  public String getStatistic(String key) throws IOException {
    return statistics.get(key);
  }

  @Override
  public void setRuntimeStatisticsSummary(RuntimeStatisticsSummary summary) throws IOException {
    this.runtimeStatisticsSummary = summary;
  }

  @Override
  public RuntimeStatisticsSummary getRuntimeStatisticsSummary() throws IOException {
    return runtimeStatisticsSummary;
  }

  @Override
  public void deleteStatistic(String key) throws IOException {
    statistics.remove(key);
  }

  @Override
  public void setEnvironmentFlags(Map<String, String> flags) {
    this.environmentFlags = flags;
  }

  @Override
  public Map<String, String> getEnvironmentFlags() {
    return environmentFlags;
  }
}
