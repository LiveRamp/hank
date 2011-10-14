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
package com.rapleaf.hank.partition_server;

import com.rapleaf.hank.config.PartitionServerConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.performance.HankTimer;
import com.rapleaf.hank.performance.HankTimerAggregator;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Implements the actual data serving logic of the PartitionServer
 */
class PartitionServerHandler implements IfaceWithShutdown {

  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankException.no_such_domain(true));

  private final static Logger LOG = Logger.getLogger(PartitionServerHandler.class);

  private final DomainAccessor[] domainAccessors;

  private final HankTimerAggregator getTimerAggregator = new HankTimerAggregator("GET", 1024);
  private final HankTimerAggregator getBulkTimerAggregator = new HankTimerAggregator("GET BULK", 1024);

  // The coordinator is supplied and not created from the configurator to allow caching
  public PartitionServerHandler(PartitionServerAddress address,
                                PartitionServerConfigurator configurator,
                                Coordinator coordinator) throws IOException {
    // Find the ring
    Ring ring = coordinator.getRingGroup(configurator.getRingGroupName()).getRingForHost(address);
    if (ring == null) {
      throw new IOException(String.format("Could not get Ring of PartitionServerAddress %s", address));
    }

    // Get the domain group for the ring
    DomainGroup domainGroup = ring.getRingGroup().getDomainGroup();
    if (domainGroup == null) {
      throw new IOException(String.format("Could not get DomainGroup of Ring %s", ring));
    }

    // Get the corresponding version number either the one we just updated to,
    // or the current one.
    Integer versionNumber = ring.getUpdatingToVersionNumber();
    if (versionNumber == null) {
      versionNumber = ring.getVersionNumber();
    }
    if (versionNumber == null) {
      throw new IOException(String.format("Could not get current version number of Ring %s", ring));
    }

    // Get the corresponding domain group version
    DomainGroupVersion domainGroupVersion = domainGroup.getVersionByNumber(versionNumber);
    if (domainGroupVersion == null) {
      throw new IOException(String.format("Could not get DomainGroupVersion of DomainGroup %s on Ring %s",
          domainGroup.toString(), ring.toString()));
    }

    // Get the corresponding Host
    Host host = ring.getHostByAddress(address);
    if (host == null) {
      throw new IOException(String.format("Could not get Host at address %s of Ring %s", address, ring));
    }

    // Determine the max domain id so we can bound the array
    int maxDomainId = 0;
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      int domainId = dgvdv.getDomain().getId();
      if (domainId > maxDomainId) {
        maxDomainId = domainId;
      }
    }
    domainAccessors = new DomainAccessor[maxDomainId + 1];

    // Loop over the domains and get set up
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      StorageEngine engine = domain.getStorageEngine();

      int domainId = dgvdv.getDomain().getId();
      HostDomain hostDomain = host.getHostDomain(domain);
      if (hostDomain == null) {
        throw new IOException(String.format("Could not get HostDomain of Domain %s on Host %s", domain, host));
      }
      Set<HostDomainPartition> partitions = hostDomain.getPartitions();
      if (partitions == null) {
        throw new IOException(String.format("Could not get partitions assignements of HostDomain %s", hostDomain));
      }
      LOG.info(String.format("Assigned %d/%d partitions in domain %s",
          partitions.size(), domain.getNumParts(), domain.getName()));

      // Instantiate the PartitionAccessor array
      PartitionAccessor[] partitionAccessors =
          new PartitionAccessor[domain.getNumParts()];
      for (HostDomainPartition partition : partitions) {
        if (partition.getCurrentDomainGroupVersion() == null) {
          LOG.error(String.format(
              "Could not load Reader for partition #%d of Domain %s because the partition's current version is null.",
              partition.getPartNum(), domain.getName()));
          continue;
        }

        // Determine at which DomainVersion the partition should be
        int domainGroupVersionDomainVersionNumber;
        try {
          DomainGroupVersion partitionDomainGroupVersion = domainGroup.getVersionByNumber(partition.getCurrentDomainGroupVersion());
          if (partitionDomainGroupVersion == null) {
            throw new IOException(String.format("Could not get version %d of Domain Group %s.",
                partition.getCurrentDomainGroupVersion(), domainGroup.getName()));
          }
          DomainGroupVersionDomainVersion domainGroupVersionDomainVersion = partitionDomainGroupVersion.getDomainVersion(domain);
          if (domainGroupVersionDomainVersion == null) {
            throw new IOException(String.format("Could not get Domain Version for Domain %s in Domain Group Version %d.",
                domain.getName(), partitionDomainGroupVersion.getVersionNumber()));
          }
          domainGroupVersionDomainVersionNumber = domainGroupVersionDomainVersion.getVersion();
        } catch (Exception e) {
          final String msg = String.format("Could not determine at which Domain Version partition #%d of Domain %s should be.",
              partition.getPartNum(), domain.getName());
          LOG.error(msg, e);
          throw new IOException(msg, e);
        }

        Reader reader = engine.getReader(configurator, partition.getPartNum());
        // Check that Reader's version number and current domain group version number match
        if (reader.getVersionNumber() != null && !reader.getVersionNumber().equals(domainGroupVersionDomainVersionNumber)) {
          final String msg = String.format("Could not load Reader for partition #%d of domain %s because version numbers reported by the Reader (%d) and by metadata (%d) differ.",
              partition.getPartNum(), domain.getName(), reader.getVersionNumber(), domainGroupVersionDomainVersionNumber);
          LOG.error(msg);
          throw new IOException(msg);
        }
        LOG.debug(String.format("Loaded partition accessor for partition #%d of domain %s with Reader " + reader,
            partition.getPartNum(), domain.getName()));
        partitionAccessors[partition.getPartNum()] = new PartitionAccessor(partition, reader);
      }
      // configure and store the DomainAccessors
      domainAccessors[domainId] = new DomainAccessor(domain.getName(), partitionAccessors, domain.getPartitioner());
    }
  }

  public HankResponse get(int domainId, ByteBuffer key) throws TException {
    HankTimer timer = getTimerAggregator.getTimer();
    try {
      DomainAccessor domainAccessor = getDomainAccessor(domainId);

      if (domainAccessor == null) {
        return NO_SUCH_DOMAIN;
      }
      try {
        return domainAccessor.get(key);
      } catch (IOException e) {
        String errMsg = String.format(
            "Exception during get! Domain: %s (domain #%d) Key: %s",
            domainAccessor.getName(), domainId, Bytes.bytesToHexString(key));
        LOG.error(errMsg, e);

        return HankResponse.xception(HankException.internal_error(errMsg + " " + e.getMessage()));
      }
    } finally {
      getTimerAggregator.add(timer);
    }
  }

  public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys) throws TException {
    HankTimer timer = getBulkTimerAggregator.getTimer();
    try {
      // Dumb implementation
      // TODO: Make it less dumb
      HankBulkResponse response = HankBulkResponse.responses(new ArrayList<HankResponse>());
      for (ByteBuffer key : keys) {
        response.get_responses().add(get(domainId, key));
      }
      return response;
    } finally {
      getBulkTimerAggregator.add(timer);
    }
  }

  private DomainAccessor getDomainAccessor(int domainId) {
    if (domainAccessors.length <= domainId) {
      return null;
    }
    return domainAccessors[domainId];
  }

  public void shutDown() throws InterruptedException {
    for (DomainAccessor domainAccessor : domainAccessors) {
      if (domainAccessor != null) {
        domainAccessor.shutDown();
      }
    }
  }
}
