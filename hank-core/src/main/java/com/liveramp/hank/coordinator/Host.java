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
package com.liveramp.hank.coordinator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.zookeeper.KeeperException;

import com.liveramp.hank.generated.RuntimeStatisticsSummary;
import com.liveramp.hank.zookeeper.WatchedNodeListener;

public interface Host extends Comparable<Host> {

  public PartitionServerAddress getAddress();

  public void setAddress(PartitionServerAddress address) throws IOException;

  public HostState getState() throws IOException;

  public void setState(HostState state) throws IOException;

  public Long getUpSince() throws IOException;

  public void setFlags(List<String> flags) throws IOException;

  public List<String> getFlags() throws IOException;

  public void setStateChangeListener(WatchedNodeListener<HostState> listener) throws IOException;

  public void cancelStateChangeListener(WatchedNodeListener<HostState> listener);

  public void enqueueCommand(HostCommand command) throws IOException;

  public List<HostCommand> getCommandQueue() throws IOException;

  public HostCommand nextCommand() throws IOException;

  public HostCommand getCurrentCommand() throws IOException;

  public void clearCommandQueue() throws IOException;

  public void setCommandQueueChangeListener(HostCommandQueueChangeListener listener) throws IOException;

  public void setCurrentCommandChangeListener(WatchedNodeListener<HostCommand> listener) throws IOException;

  public Set<HostDomain> getAssignedDomains() throws IOException;

  public SortedSet<HostDomain> getAssignedDomainsSorted() throws IOException;

  public HostDomain addDomain(Domain domain) throws IOException;

  public void removeDomain(Domain domain) throws IOException;

  public HostDomain getHostDomain(Domain domain);

  public void setEphemeralStatistic(String key, String value) throws IOException;

  public String getStatistic(String key) throws IOException;

  public void setRuntimeStatisticsSummary(RuntimeStatisticsSummary summary) throws IOException;

  public RuntimeStatisticsSummary getRuntimeStatisticsSummary() throws IOException;

  public void deleteStatistic(String key) throws IOException;

  public void setEnvironmentFlags(Map<String, String> flags);

  public Map<String, String> getEnvironmentFlags();
}
