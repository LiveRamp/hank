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
import com.rapleaf.hank.storage.Result;
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
  private static final HankResponse WRONG_HOST = HankResponse
      .xception(HankExceptions.wrong_host(true));

  private static final HankResponse NOT_FOUND = HankResponse.not_found(true);

  private static final HankResponse NO_SUCH_DOMAIN = HankResponse
      .xception(HankExceptions.no_such_domain(true));

  private final static Logger LOG = Logger.getLogger(PartitionServerHandler.class);

  private final DomainAccessor[] domains;

  public PartitionServerHandler(PartitionServerAddress hostAndPort,
                                PartitionServerConfigurator config) throws IOException {
    // find the ring config
    Ring ringConfig = config.getCoordinator()
        .getRingGroup(config.getRingGroupName())
        .getRingForHost(hostAndPort);

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

    domains = new DomainAccessor[maxDomainId + 1];

    // loop over the domains and get set up
    for (DomainGroupVersionDomainVersion domainVersion : domainGroup.getLatestVersion()
        .getDomainVersions()) {
      Domain domain = domainVersion.getDomain();
      StorageEngine engine = domain.getStorageEngine();

      int domainId = domainGroup.getDomainId(domain.getName());
      Set<HostDomainPartition> partitions = ringConfig
          .getHostByAddress(hostAndPort).getDomainById(domainId)
          .getPartitions();
      LOG.info(String.format("Assigned %d/%d partitions in domain %s",
          partitions.size(), domain.getNumParts(), domain.getName()));

      // instantiate all the PartitionAccessor
      PartitionAccessor[] readersAndCounters =
          new PartitionAccessor[domain.getNumParts()];
      for (HostDomainPartition part : partitions) {
        LOG.debug(String.format(
            "Instantiating PartitionAccessor for part num %d",
            part.getPartNum()));
        readersAndCounters[part.getPartNum()] = new PartitionAccessor(part, engine.getReader(
            config, part.getPartNum()));
      }

      // configure and store the Domain wrapper
      domains[domainId] = new DomainAccessor(domain.getName(), readersAndCounters,
          domain.getPartitioner());
    }
  }

  public HankResponse get(int domainId, ByteBuffer key) throws TException {
    Result result = new Result();
    DomainAccessor domain = getDomain(domainId & 0xff);

    if (domain == null) {
      return NO_SUCH_DOMAIN;
    }

    try {
      if (domain.get(key, result)) {
        if (result.isFound()) {
          return HankResponse.value(result.getBuffer());
        } else {
          return NOT_FOUND;
        }
      } else {
        return WRONG_HOST;
      }
    } catch (IOException e) {
      String errMsg = String.format(
          "Exception during get! Domain: %d (%s) Key: %s", domainId,
          domain.getName(), Bytes.bytesToHexString(key));
      LOG.error(errMsg, e);

      return HankResponse.xception(HankExceptions.internal_error(errMsg));
    }
  }

  private DomainAccessor getDomain(int domainId) {
    if (domains.length <= domainId) {
      return null;
    }
    return domains[domainId];
  }

  public void shutDown() throws InterruptedException {
    for (DomainAccessor currentDomain : domains) {
      currentDomain.shutDown();
    }
  }
}
