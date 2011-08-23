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
package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class MockHost extends AbstractHost {
  private final PartitionServerAddress address;
  private HostState state = HostState.OFFLINE;
  private List<HostCommand> commandQueue = new LinkedList<HostCommand>();
  private HostCommand currentCommand;

  public MockHost(PartitionServerAddress address) {
    this.address = address;
  }

  @Override
  public HostDomain addDomain(int domainId) throws IOException {
    return null;
  }

  @Override
  public PartitionServerAddress getAddress() {
    return address;
  }

  @Override
  public Set<HostDomain> getAssignedDomains() throws IOException {
    return null;
  }

  @Override
  public HostDomain getDomainById(int domainId) {
    return null;
  }

  @Override
  public HostState getState() throws IOException {
    return state;
  }

  @Override
  public void setStateChangeListener(HostStateChangeListener listener) {
  }

  @Override
  public HostCommand getCurrentCommand() throws IOException {
    return currentCommand;
  }

  @Override
  public boolean isOnline() throws IOException {
    return state != HostState.OFFLINE;
  }

  @Override
  public void setState(HostState state) throws IOException {
    this.state = state;
  }

  @Override
  public void completeCommand() throws IOException {
    currentCommand = null;
  }

  @Override
  public void enqueueCommand(HostCommand command) throws IOException {
    commandQueue.add(command);
  }

  @Override
  public List<HostCommand> getCommandQueue() throws IOException {
    return commandQueue;
  }

  @Override
  public HostCommand processNextCommand() throws IOException {
    currentCommand = commandQueue.remove(0);
    return currentCommand;
  }

  @Override
  public void setCommandQueueChangeListener(
      HostCommandQueueChangeListener listener) throws IOException {
  }

  @Override
  public void cancelStateChangeListener(HostStateChangeListener listener) {
  }

  @Override
  public void clearCommandQueue() throws IOException {
    commandQueue.clear();
  }
}
