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
import com.rapleaf.hank.generated.HankExceptions;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Implements the actual data serving logic of the PartitionServer
 */
class PartitionServerHandler implements IfaceWithShutdown {

  private static final HankResponse NO_SUCH_DOMAIN = HankResponse
      .xception(HankExceptions.no_such_domain(true));

  private final static Logger LOG = Logger.getLogger(PartitionServerHandler.class);

  private final DomainAccessor[] domainAccessors;

  public PartitionServerHandler(PartitionServerAddress address,
                                PartitionServerConfigurator configurator) throws IOException {
    // find the ring config
    Ring ringConfig = configurator.getCoordinator()
        .getRingGroup(configurator.getRingGroupName())
        .getRingForHost(address);

    // get the domain group config for the ring
    DomainGroup domainGroup = ringConfig.getRingGroup().getDomainGroup();

    // determine the max domain id so we can bound the array
    int maxDomainId = 0;
    for (DomainGroupVersionDomainVersion dcv : domainGroup.getLatestVersion()
        .getDomainVersions()) {
      int domainId = domainGroup.getDomainId(dcv.getDomain().getName());
      if (domainId > maxDomainId) {
        maxDomainId = domainId;
      }
    }

    domainAccessors = new DomainAccessor[maxDomainId + 1];

    // loop over the domains and get set up
    for (DomainGroupVersionDomainVersion domainVersion :
        domainGroup.getLatestVersion().getDomainVersions()) {
      Domain domain = domainVersion.getDomain();
      StorageEngine engine = domain.getStorageEngine();

      int domainId = domainGroup.getDomainId(domain.getName());
      Set<HostDomainPartition> partitions = ringConfig
          .getHostByAddress(address).getDomainById(domainId)
          .getPartitions();
      LOG.info(String.format("Assigned %d/%d partitions in domain %s",
          partitions.size(), domain.getNumParts(), domain.getName()));

      // instantiate the PartitionAccessor array
      PartitionAccessor[] partitionAccessors =
          new PartitionAccessor[domain.getNumParts()];
      for (HostDomainPartition partition : partitions) {
        if (partition.getCurrentDomainGroupVersion() == null) {
          LOG.error(String.format(
              "Could not load Reader for partition #%d because its current version is null.",
              partition.getPartNum()));
          continue;
        }
        Reader reader = engine.getReader(configurator, partition.getPartNum());
        // Check that Reader's version number and current domain group version number matchte
        if (reader.getVersionNumber() != null && !reader.getVersionNumber().equals(partition.getCurrentDomainGroupVersion())) {
          LOG.error(String.format("Could not load Reader for partition %d because version numbers reported by the Reader (%d) and by metadata (%d) differ.",
              partition.getPartNum(), reader.getVersionNumber(), partition.getCurrentDomainGroupVersion()));
          reader = null;
        }
        LOG.debug(String.format("Loaded partition accessor for partition #%d with Reader " + reader, partition.getPartNum()));
        partitionAccessors[partition.getPartNum()] = new PartitionAccessor(partition, reader);
      }
      // configure and store the DomainAccessors
      domainAccessors[domainId] = new DomainAccessor(domain.getName(), partitionAccessors, domain.getPartitioner());
    }
  }

  public HankResponse get(int domainId, ByteBuffer key) throws TException {
    DomainAccessor domainAccessor = getDomainAccessor(domainId & 0xff);

    if (domainAccessor == null) {
      return NO_SUCH_DOMAIN;
    }
    try {
      return domainAccessor.get(key);
    } catch (IOException e) {
      String errMsg = String.format(
          "Exception during get! Domain: %d (%s) Key: %s", domainId,
          domainAccessor.getName(), Bytes.bytesToHexString(key));
      LOG.error(errMsg, e);

      return HankResponse.xception(HankExceptions.internal_error(errMsg + " " + e.getMessage()));
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
      domainAccessor.shutDown();
    }
  }
}
