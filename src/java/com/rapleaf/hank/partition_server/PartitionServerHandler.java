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
import com.rapleaf.hank.storage.ReaderResult;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Implements the actual data serving logic of the PartitionServer
 */
class PartitionServerHandler implements IfaceWithShutdown {

  private final static Logger LOG = Logger.getLogger(PartitionServerHandler.class);

  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankException.no_such_domain(true));
  private static final HankBulkResponse NO_SUCH_DOMAIN_BULK = HankBulkResponse.xception(HankException.no_such_domain(true));
  private static final HankResponse INTERRUPTED_GET = HankResponse.xception(
      HankException.internal_error("Interrupted while waiting for GET to complete."));
  private static final HankBulkResponse INTERRUPTED_GET_BULK = HankBulkResponse.xception(
      HankException.internal_error("Interrupted while waiting for GET BULK to complete."));
  private final int getBulkTaskSize;
  private final long GET_BULK_TASK_EXECUTOR_KEEP_ALIVE_VALUE = 1;
  private final TimeUnit GET_BULK_TASK_EXECUTOR_KEEP_ALIVE_UNIT = TimeUnit.DAYS;

  private final HankTimerAggregator getTimerAggregator;
  private final HankTimerAggregator getBulkTimerAggregator;

  private final ThreadPoolExecutor getBulkTaskExecutor;
  private final DomainAccessor[] domainAccessors;
  private static final long GET_BULK_TASK_EXECUTOR_AWAIT_TERMINATION_VALUE = 1;
  private static final TimeUnit GET_BULK_TASK_EXECUTOR_AWAIT_TERMINATION_UNIT = TimeUnit.SECONDS;

  // The coordinator is supplied and not created from the configurator to allow caching
  public PartitionServerHandler(PartitionServerAddress address,
                                PartitionServerConfigurator configurator,
                                Coordinator coordinator) throws IOException {
    // Create the GET executor
    getBulkTaskExecutor = new ThreadPoolExecutor(
        configurator.getNumConcurrentGetBulkTasks(),
        configurator.getNumConcurrentGetBulkTasks(),
        GET_BULK_TASK_EXECUTOR_KEEP_ALIVE_VALUE,
        GET_BULK_TASK_EXECUTOR_KEEP_ALIVE_UNIT,
        new LinkedBlockingQueue<Runnable>(),
        new GetBulkThreadFactory());

    getBulkTaskSize = configurator.getGetBulkTaskSize();

    // Prestart core threads
    getBulkTaskExecutor.prestartAllCoreThreads();

    // Set up timer aggregators
    getTimerAggregator = new HankTimerAggregator("GET", configurator.getGetTimerAggregatorWindow());
    getBulkTimerAggregator = new HankTimerAggregator("GET BULK", configurator.getGetBulkTimerAggregatorWindow());

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
      versionNumber = ring.getCurrentVersionNumber();
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
      LOG.info(String.format("Loading %d/%d partitions of domain %s",
          partitions.size(), domain.getNumParts(), domain.getName()));

      // Instantiate the PartitionAccessor array
      PartitionAccessor[] partitionAccessors =
          new PartitionAccessor[domain.getNumParts()];
      for (HostDomainPartition partition : partitions) {
        if (partition.getCurrentDomainGroupVersion() == null) {
          LOG.error(String.format(
              "Could not load Reader for partition #%d of Domain %s because the partition's current version is null.",
              partition.getPartitionNumber(), domain.getName()));
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
              partition.getPartitionNumber(), domain.getName());
          LOG.error(msg, e);
          throw new IOException(msg, e);
        }

        Reader reader = engine.getReader(configurator, partition.getPartitionNumber());
        // Check that Reader's version number and current domain group version number match
        if (reader.getVersionNumber() != null && !reader.getVersionNumber().equals(domainGroupVersionDomainVersionNumber)) {
          final String msg = String.format("Could not load Reader for partition #%d of domain %s because version numbers reported by the Reader (%d) and by metadata (%d) differ.",
              partition.getPartitionNumber(), domain.getName(), reader.getVersionNumber(), domainGroupVersionDomainVersionNumber);
          LOG.error(msg);
          throw new IOException(msg);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Loaded partition accessor for partition #%d of domain %s with Reader " + reader,
              partition.getPartitionNumber(), domain.getName()));
        }
        partitionAccessors[partition.getPartitionNumber()] = new PartitionAccessor(partition, reader);
      }
      // configure and store the DomainAccessors
      domainAccessors[domainId] = new DomainAccessor(domain.getName(), partitionAccessors, domain.getPartitioner());
    }
  }

  public HankResponse get(int domainId, ByteBuffer key) {
    HankTimer timer = getTimerAggregator.getTimer();
    try {
      // TODO: re-use result
      ReaderResult result = new ReaderResult();
      return _get(this, domainId, key, result);
    } finally {
      getTimerAggregator.add(timer);
    }
  }

  public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys) {
    HankTimer timer = getBulkTimerAggregator.getTimer();
    try {
      DomainAccessor domainAccessor = getDomainAccessor(domainId);
      if (domainAccessor == null) {
        return NO_SUCH_DOMAIN_BULK;
      }
      // Build and execute all get bulk tasks
      HankBulkResponse bulkResponse = HankBulkResponse.responses(new ArrayList<HankResponse>(keys.size()));
      GetBulkTask[] tasks = new GetBulkTask[(keys.size() / getBulkTaskSize) + 1];
      int maxTaskIndex = 0;
      for (int i = 0; i < keys.size(); i += getBulkTaskSize) {
        GetBulkTask task = new GetBulkTask(new GetBulkRunnable(domainId, keys, i));
        // No need to synchronize since ThreadPoolExecutor's execute() is thread-safe
        getBulkTaskExecutor.execute(task);
        tasks[maxTaskIndex++] = task;
      }
      // Wait for all get tasks and retrieve responses
      for (int taskIndex = 0; taskIndex < maxTaskIndex; ++taskIndex) {
        HankResponse[] responses = tasks[taskIndex].getResponses();
        for (HankResponse response : responses) {
          // Check if we have retrieved all responses
          if (bulkResponse.get_responses().size() == keys.size()) {
            break;
          } else {
            bulkResponse.get_responses().add(response);
          }
        }
      }
      return bulkResponse;
    } catch (Throwable t) {
      String errMsg = "Throwable during GET BULK";
      LOG.fatal(errMsg, t);
      return HankBulkResponse.xception(
          HankException.internal_error(errMsg + " " + (t.getMessage() != null ? t.getMessage() : "")));
    } finally {
      getBulkTimerAggregator.add(timer, keys.size());
    }
  }

  private HankResponse _get(PartitionServerHandler partitionServerHandler, int domainId, ByteBuffer key, ReaderResult result) {
    DomainAccessor domainAccessor = partitionServerHandler.getDomainAccessor(domainId);
    if (domainAccessor == null) {
      return NO_SUCH_DOMAIN;
    }
    try {
      return domainAccessor.get(key, result);
    } catch (IOException e) {
      String errMsg = String.format(
          "Exception during GET. Domain: %s (domain #%d) Key: %s",
          domainAccessor.getName(), domainId, Bytes.bytesToHexString(key));
      LOG.error(errMsg, e);
      return HankResponse.xception(
          HankException.internal_error(errMsg + " " + (e.getMessage() != null ? e.getMessage() : "")));
    } catch (Throwable t) {
      String errMsg = "Throwable during GET";
      LOG.fatal(errMsg, t);
      return HankResponse.xception(
          HankException.internal_error(errMsg + " " + (t.getMessage() != null ? t.getMessage() : "")));
    }
  }

  private static class GetThread extends Thread {

    private ReaderResult result = new ReaderResult();

    public GetThread(Runnable runnable, String name) {
      super(runnable, name);
    }

    public ReaderResult getResult() {
      return result;
    }
  }

  private static class GetBulkThreadFactory implements ThreadFactory {

    private int threadId = 0;

    @Override
    public Thread newThread(Runnable runnable) {
      return new GetThread(runnable, "GET BULK Thread " + threadId++);
    }
  }

  private class GetBulkRunnable implements Runnable {

    private final int domainId;
    private final List<ByteBuffer> keys;
    private final int firstKeyIndex;
    private HankResponse[] responses;

    // Perform GET requests for keys starting at firstKeyIndex and in a window of size GET_BULK_TASK_SIZE
    public GetBulkRunnable(int domainId, List<ByteBuffer> keys, int firstKeyIndex) {
      this.domainId = domainId;
      this.keys = keys;
      this.firstKeyIndex = firstKeyIndex;
    }

    @Override
    public void run() {
      ReaderResult result = ((GetThread) Thread.currentThread()).getResult();
      result.clear();
      responses = new HankResponse[getBulkTaskSize];
      // Perform GET requests for keys starting at firstKeyIndex up to GET_BULK_TASK_SIZE keys or until the last key
      for (int keyOffset = 0; keyOffset < getBulkTaskSize
          && (firstKeyIndex + keyOffset) < keys.size(); keyOffset++) {
        responses[keyOffset] =
            _get(PartitionServerHandler.this, domainId, keys.get(firstKeyIndex + keyOffset), result);
      }
    }

    public HankResponse[] getResponses() {
      return responses;
    }
  }

  private class GetBulkTask extends FutureTask<Object> {

    private final GetBulkRunnable runnable;

    public GetBulkTask(GetBulkRunnable runnable) {
      super(runnable, new Object());
      this.runnable = runnable;
    }

    // Wait for termination and return response
    public HankResponse[] getResponses() throws ExecutionException, InterruptedException {
      this.get();
      return runnable.getResponses();
    }
  }

  private DomainAccessor getDomainAccessor(int domainId) {
    if (domainAccessors.length <= domainId) {
      return null;
    }
    return domainAccessors[domainId];
  }

  public void shutDown() {
    // Shutdown GET tasks
    getBulkTaskExecutor.shutdown();
    try {
      while (!getBulkTaskExecutor.awaitTermination(GET_BULK_TASK_EXECUTOR_AWAIT_TERMINATION_VALUE,
          GET_BULK_TASK_EXECUTOR_AWAIT_TERMINATION_UNIT)) {
        LOG.debug("Waiting for termination of GET BULK task executor during shutdown.");
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for termination of GET BULK task executor during shutdown.");
    }
    // Shutdown domain accessors
    for (DomainAccessor domainAccessor : domainAccessors) {
      if (domainAccessor != null) {
        domainAccessor.shutDown();
      }
    }
  }
}
