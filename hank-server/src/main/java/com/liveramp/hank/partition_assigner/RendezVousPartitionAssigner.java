/**
 *  Copyright 2013 LiveRamp
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

package com.liveramp.hank.partition_assigner;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.hasher.Murmur64Hasher;

public class RendezVousPartitionAssigner extends AbstractMappingPartitionAssigner implements PartitionAssigner {

  @Override
  protected Map<Integer, Host> getPartitionsAssignment(Domain domain, SortedSet<Host> validHosts) {
    Map<Host, List<Integer>> hostToPartitions = getHostToPartitions(domain, validHosts);
    // Compute result
    Map<Integer, Host> result = new HashMap<Integer, Host>();
    for (Map.Entry<Host, List<Integer>> entry : hostToPartitions.entrySet()) {
      for (Integer partitionNumber : entry.getValue()) {
        result.put(partitionNumber, entry.getKey());
      }
    }
    return result;
  }

  private Map<Host, List<Integer>> getHostToPartitions(Domain domain, SortedSet<Host> validHosts) {
    int maxPartitionsPerHost = getMaxPartitionsPerHost(domain, validHosts);
    Map<Host, List<Integer>> result = new HashMap<Host, List<Integer>>();
    // Initialize empty mappings
    for (Host host : validHosts) {
      result.put(host, new ArrayList<Integer>());
    }
    for (int partitionNumber = 0; partitionNumber < domain.getNumParts(); ++partitionNumber) {
      // Assign to hosts by order of increasing weight
      List<Host> orderedHosts = getOrderedWeightedHosts(domain, partitionNumber, validHosts);
      boolean assigned = false;
      for (Host host : orderedHosts) {
        // If there is room, assign, otherwise skip to next host
        List<Integer> assignedPartitions = result.get(host);
        if (assignedPartitions.size() < maxPartitionsPerHost) {
          assignedPartitions.add(partitionNumber);
          assigned = true;
          break;
        }
      }
      if (!assigned) {
        throw new RuntimeException("Partition should have been assigned but wasn't. This should never happen.");
      }
    }
    return result;
  }

  private static class HostAndPartitionRendezVous implements Comparable<HostAndPartitionRendezVous> {

    private final Host host;
    private final Long rendezVousHashValue;

    private HostAndPartitionRendezVous(Domain domain, int partitionId, Host host) {
      this.host = host;
      this.rendezVousHashValue = computeRendezVousHashValue(domain, partitionId, host);
    }

    // Technique based on Rendez-Vous Hashing (achieves a result similar to consistent hashing)
    private long computeRendezVousHashValue(Domain domain, int partitionId, Host host) {
      byte[] hostAddress;
      try {
        hostAddress = host.getAddress().toString().getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
      ByteBuffer value = ByteBuffer.allocate(4 + 4 + hostAddress.length)
          .order(ByteOrder.LITTLE_ENDIAN)
          .putInt(partitionId)
          .putInt(domain.getId())
          .put(hostAddress);
      value.flip();
      return Murmur64Hasher.murmurHash64(value);
    }

    @Override
    public int compareTo(HostAndPartitionRendezVous o) {
      return rendezVousHashValue.compareTo(o.rendezVousHashValue);
    }
  }

  private List<Host> getOrderedWeightedHosts(Domain domain, int partitionNumber, SortedSet<Host> validHosts) {
    List<HostAndPartitionRendezVous> hostAndPartitionRendezVousList = new ArrayList<HostAndPartitionRendezVous>();
    for (Host host : validHosts) {
      hostAndPartitionRendezVousList.add(new HostAndPartitionRendezVous(domain, partitionNumber, host));
    }
    // Sort by rendez vous hash values
    Collections.sort(hostAndPartitionRendezVousList);
    // Build result
    List<Host> result = new ArrayList<Host>();
    for (HostAndPartitionRendezVous hostAndPartitionRendezVous : hostAndPartitionRendezVousList) {
      result.add(hostAndPartitionRendezVous.host);
    }
    return result;
  }

  private int getMaxPartitionsPerHost(Domain domain, SortedSet<Host> validHosts) {
    if (domain.getNumParts() % validHosts.size() == 0) {
      return domain.getNumParts() / validHosts.size();
    } else {
      return (domain.getNumParts() / validHosts.size()) + 1;
    }
  }
}
