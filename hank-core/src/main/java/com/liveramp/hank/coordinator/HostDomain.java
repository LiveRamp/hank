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
import java.util.Set;
import java.util.SortedSet;

public interface HostDomain extends Comparable<HostDomain> {

  public Domain getDomain();

  public Set<HostDomainPartition> getPartitions() throws IOException;

  public SortedSet<HostDomainPartition> getPartitionsSorted() throws IOException;

  public HostDomainPartition getPartitionByNumber(int partNum) throws IOException;

  public HostDomainPartition addPartition(int partNum) throws IOException;

  public void removePartition(int partNum) throws IOException;
}
