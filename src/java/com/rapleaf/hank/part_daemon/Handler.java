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
package com.rapleaf.hank.part_daemon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.rapleaf.hank.config.DomainConfig;
import com.rapleaf.hank.config.DomainConfigVersion;
import com.rapleaf.hank.config.DomainGroupConfig;
import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.config.PartDaemonConfigurator;
import com.rapleaf.hank.config.RingConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartDaemon.Iface;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.Result;
import com.rapleaf.hank.storage.StorageEngine;

public class Handler implements Iface {
  private final static Logger LOG = Logger.getLogger(Handler.class);

  private final Domain[] domains;

  public Handler(PartDaemonAddress hostAndPort, PartDaemonConfigurator config) throws DataNotFoundException, IOException {
    // find the ring config
    RingConfig ringConfig = config.getCoordinator().getRingConfig(config.getRingGroupName(), config.getRingNumber());

    // get the domain group config for the ring
    DomainGroupConfig domainGroupConfig = ringConfig.getRingGroupConfig().getDomainGroupConfig();

    // determine the max domain id so we can bound the array
    int maxDomainId = 0;
    for (DomainConfigVersion dcv: domainGroupConfig.getLatestVersion().getDomainConfigVersions()) {
      int domainId = domainGroupConfig.getDomainId(dcv.getDomainConfig().getName());
      if (domainId > maxDomainId) {
        maxDomainId = domainId;
      }
    }

    domains = new Domain[maxDomainId + 1];

    // loop over the domains and get set up
    for (DomainConfigVersion dcv: domainGroupConfig.getLatestVersion().getDomainConfigVersions()) {
      DomainConfig domainConfig = dcv.getDomainConfig();
      StorageEngine eng = domainConfig.getStorageEngine();

      int domainId = domainGroupConfig.getDomainId(domainConfig.getName());
      Set<Integer> partitions = ringConfig.getDomainPartitionsForHost(hostAndPort, domainId);
      LOG.info(String.format("Assigned %d/%d partitions in domain %s",
          partitions.size(),
          domainConfig.getNumParts(),
          domainConfig.getName()));

      // instantiate all the readers
      Reader[] readers = new Reader[domainConfig.getNumParts()];
      for (int partNum : partitions) {
        LOG.debug(String.format("Instantiating reader for part num %d", partNum));
        readers[partNum] = eng.getReader(config, partNum);
      }

      // configure and store the Domain wrapper
      domains[domainId] = new Domain(domainConfig.getName(), readers, domainConfig.getPartitioner());
    }
  }

  @Override
  public HankResponse get(byte domainId, ByteBuffer key) throws TException {
    Result result = new Result();
    Domain domain = getDomain(domainId & 0xff);

    if (domain == null) {
      return HankResponse.no_such_domain(true);
    }

    try {
      if (domain.get(key, result)) {
        if (result.isFound()) {
          return HankResponse.value(result.getBuffer());
        } else {
          return HankResponse.not_found(true);
        }
      } else {
        return HankResponse.wrong_host(true);
      }
    } catch (IOException e) {
      LOG.error(String.format("Exception during get! Domain: %d (%s) Key: %s",
          domainId,
          domain.getName(),
          stringifyKey(key)), e);

      return HankResponse.internal_error(true);
    }
  }

  private static String stringifyKey(ByteBuffer key) {
    int off = key.arrayOffset() + key.position();
    int lim = key.limit();
    if (key.remaining() > 64) {
      lim = off + 64;
    }

    StringBuilder sb = new StringBuilder();
    for (int i = off; i < lim; i++) {
      sb.append(String.format("%x", key.array()[i] & 0xff));
    }
    if (key.remaining() > 64) {
      sb.append(" (truncated)");
    }
    return sb.toString();
  }

  private Domain getDomain(int domainId) {
    if (domains.length <= domainId) {
      return null;
    }
    return domains[domainId];
  }
}
