/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.liveramp.hank.partition_server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankException;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.storage.Reader;
import com.liveramp.hank.storage.ReaderResult;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.util.UpdateStatisticsRunnable;

/**
 * Implements the actual data serving logic of the PartitionServer
 */
public class PartitionServerHandler implements IfaceWithShutdown {

  private final static Logger LOG = LoggerFactory.getLogger(PartitionServerHandler.class);

  private final Host host;
  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankException.no_such_domain(true));
  private static final HankBulkResponse NO_SUCH_DOMAIN_BULK = HankBulkResponse.xception(HankException.no_such_domain(true));
  private final int getBulkTaskSize;
  private final int bufferReuseMaxSize;
  private static final long GET_BULK_TASK_EXECUTOR_KEEP_ALIVE_VALUE = 1;
  private static final TimeUnit GET_BULK_TASK_EXECUTOR_KEEP_ALIVE_UNIT = TimeUnit.DAYS;

  private static final ReaderResultThreadLocal readerResultThreadLocal = new ReaderResultThreadLocal();
  private final DomainAccessor[] domainAccessors;
  private final ThreadPoolExecutor getBulkTaskExecutor;
  private static final long GET_BULK_TASK_EXECUTOR_AWAIT_TERMINATION_VALUE = 1;
  private static final TimeUnit GET_BULK_TASK_EXECUTOR_AWAIT_TERMINATION_UNIT = TimeUnit.SECONDS;
  private static final double USED_SIZE_THRESHOLD_FOR_VALUE_BUFFER_DEEP_COPY = 0.75;

  private final UpdateStatisticsRunnable updateRuntimeStatisticsRunnable;
  private final Thread updateRuntimeStatisticsThread;
  private static final int UPDATE_RUNTIME_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT = 30000;

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
    bufferReuseMaxSize = configurator.getBufferReuseMaxSize();

    // Prestart core threads
    getBulkTaskExecutor.prestartAllCoreThreads();

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

    // Get the corresponding Host
    host = ring.getHostByAddress(address);
    if (host == null) {
      throw new IOException(String.format("Could not get Host at address %s of Ring %s", address, ring));
    }

    // Compute the total number of partitions that are going to be loaded
    int numTotalPartitions = 0;
    for (DomainAndVersion dgdv : domainGroup.getDomainVersions()) {
      HostDomain hostDomain = host.getHostDomain(dgdv.getDomain());
      if (hostDomain != null) {
        Set<HostDomainPartition> partitions = hostDomain.getPartitions();
        if (partitions != null) {
          for (HostDomainPartition partition : partitions) {
            if (partition.getCurrentDomainVersion() != null) {
              numTotalPartitions += 1;
            }
          }
        }
      }
    }
    LOG.info("Detected " + numTotalPartitions + " loadable partitions for " + host);

    // Determine the max domain id so we can bound the arrays
    int maxDomainId = 0;
    for (DomainAndVersion dgvdv : domainGroup.getDomainVersions()) {
      int domainId = dgvdv.getDomain().getId();
      if (domainId > maxDomainId) {
        maxDomainId = domainId;
      }
    }
    domainAccessors = new DomainAccessor[maxDomainId + 1];

    // Loop over the domains and get set up
    List<Exception> exceptions = new ArrayList<Exception>();
    for (DomainAndVersion dgvdv : domainGroup.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      StorageEngine engine = domain.getStorageEngine();

      int domainId = dgvdv.getDomain().getId();
      HostDomain hostDomain = host.getHostDomain(domain);
      if (hostDomain == null) {
        LOG.error(String.format("Could not get HostDomain of Domain %s on Host %s. Skipping.", domain, host));
        continue;
      }
      Set<HostDomainPartition> partitions = hostDomain.getPartitions();
      if (partitions == null) {
        LOG.error(String.format("Could not get partitions assignements of HostDomain %s. Skipping.", hostDomain));
        continue;
      }

      LOG.info(String.format("Loading %d/%d partitions of domain %s",
          partitions.size(), domain.getNumParts(), domain.getName()));

      // Instantiate the PartitionAccessor array
      PartitionAccessor[] partitionAccessors =
          new PartitionAccessor[domain.getNumParts()];

      Set<Integer> partitionNumbers = Sets.newHashSet();
      for (HostDomainPartition partition : partitions) {
        partitionNumbers.add(partition.getPartitionNumber());
      }

      DiskPartitionAssignment assignment = engine.getDataDirectoryPerPartition(configurator, partitionNumbers);

      for (HostDomainPartition partition : partitions) {
        if (partition.getCurrentDomainVersion() == null) {
          LOG.error(String.format(
              "Could not load Reader for partition #%d of Domain %s because the partition's current version is null.",
              partition.getPartitionNumber(), domain.getName()));
          continue;
        }

        Reader reader;
        try {
          reader = engine.getReader(configurator.getReaderConfigurator(numTotalPartitions), partition.getPartitionNumber(), assignment);
        } catch (IOException | IllegalArgumentException e) {
          // Something went wrong when loading this partition's Reader. Set it deletable and signal failure.
          if (!partition.isDeletable()) {
            partition.setDeletable(true);
          }
          final String msg = String.format("Could not load Reader for partition #%d of domain %s because of an exception.",
              partition.getPartitionNumber(), domain.getName());
          LOG.error(msg, e);
          exceptions.add(new IOException(msg, e));
          continue;
        }
        // Check that Reader's version number and current domain group version number match
        if (reader.getVersionNumber() != null && !reader.getVersionNumber().equals(partition.getCurrentDomainVersion())) {
          // Something went wrong when loading this partition's Reader. Set it deletable and signal failure.
          partition.setDeletable(true);
          final String msg = String.format("Could not load Reader for partition #%d of domain %s because version numbers reported by the Reader (%d) and by metadata (%d) differ.",
              partition.getPartitionNumber(), domain.getName(), reader.getVersionNumber(), partition.getCurrentDomainVersion());
          LOG.error(msg);
          exceptions.add(new IOException(msg));
          continue;
        }
        LOG.info(String.format("Loaded Reader for domain %s, partition #%d",
            domain.getName(), partition.getPartitionNumber()));
        partitionAccessors[partition.getPartitionNumber()] = new PartitionAccessor(partition, reader);
        // Loading succeeded, if the partition was deletable, it should not be
        if (partition.isDeletable()) {
          partition.setDeletable(false);
        }
      }
      // configure and store the DomainAccessors
      domainAccessors[domainId] = new DomainAccessor(hostDomain, partitionAccessors, domain.getPartitioner(),
          configurator.getGetTimerAggregatorWindow());
    }
    // If there was a failure, abort and skip current command.
    if (!exceptions.isEmpty()) {
      host.nextCommand();
      throw new IOException("Failed to load Readers. Encountered " + exceptions.size() + " exceptions.");
    }
    // Start the update runtime statistics thread
    updateRuntimeStatisticsRunnable = new UpdateRuntimeStatisticsRunnable();
    updateRuntimeStatisticsThread = new Thread(updateRuntimeStatisticsRunnable, "Update Runtime Statistics");
    updateRuntimeStatisticsThread.start();
  }

  @Override
  public HankResponse get(int domainId, ByteBuffer key) {
    ReaderResult result = readerResultThreadLocal.get();
    // If buffer exceeds limit, reset it
    if (result.getBuffer() != null && result.getBuffer().capacity() > bufferReuseMaxSize) {
      readerResultThreadLocal.remove();
      result = readerResultThreadLocal.get();
    }
    result.clear();
    return _get(this, domainId, key, result);
  }

  @Override
  public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys) {
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
      LOG.error(errMsg, t);
      return HankBulkResponse.xception(
          HankException.internal_error(errMsg + " " + (t.getMessage() != null ? t.getMessage() : "")));
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
          domainAccessor.getName(), domainId, BytesUtils.bytesToHexString(key));
      LOG.error(errMsg, e);
      return HankResponse.xception(
          HankException.internal_error(errMsg + " " + (e.getMessage() != null ? e.getMessage() : "")));
    } catch (Throwable t) {
      String errMsg = "Throwable during GET";
      LOG.error(errMsg, t);
      return HankResponse.xception(
          HankException.internal_error(errMsg + " " + (t.getMessage() != null ? t.getMessage() : "")));
    }
  }

  private static class ReaderResultThreadLocal extends ThreadLocal<ReaderResult> {

    @Override
    protected ReaderResult initialValue() {
      return new ReaderResult();
    }
  }

  private static class GetThread extends Thread {

    public GetThread(Runnable runnable, String name) {
      super(runnable, name);
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
      ReaderResult result = readerResultThreadLocal.get();
      result.clear();
      responses = new HankResponse[getBulkTaskSize];
      // Perform GET requests for keys starting at firstKeyIndex up to GET_BULK_TASK_SIZE keys or until the last key
      for (int keyOffset = 0; keyOffset < getBulkTaskSize
          && (firstKeyIndex + keyOffset) < keys.size(); keyOffset++) {
        HankResponse response =
            _get(PartitionServerHandler.this, domainId, keys.get(firstKeyIndex + keyOffset), result);
        // If a value was found, we have the choice to keep the buffer that was used to read the value, or do a deep
        // copy into the response. This decision is based on a size difference threshold.
        // This allows us to do bulk requests that are large even when the read buffer ends up being much larger
        // than the stored value (since in that case we will just do a deep copy of the value
        // in an appropriately-sized buffer).
        if (response.is_set_value()) {
          ByteBuffer valueBuffer = response.buffer_for_value();
          // If buffer used space is less than a threshold times its capacity, do a deep copy.
          if (((double)valueBuffer.limit())
              < (USED_SIZE_THRESHOLD_FOR_VALUE_BUFFER_DEEP_COPY * valueBuffer.capacity())) {
            // Deep copy the value. Hence we can reuse the result buffer.
            response.set_value(BytesUtils.byteBufferDeepCopy(valueBuffer));
            result.clear();
          } else {
            // Keep the ReaderResult's buffer in the response. Hence we need to create a new result buffer.
            // Initialize it with the same capacity we had.
            result = new ReaderResult(valueBuffer.capacity());
          }
        }
        // Store response
        responses[keyOffset] = response;
      }
      // Update the thread local result buffer to point to the latest one used (which is valid for reuse)
      readerResultThreadLocal.set(result);
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
    if (domainId < domainAccessors.length) {
      return domainAccessors[domainId];
    } else {
      return null;
    }
  }

  /**
   * This thread periodically updates statistics of the Host
   */
  private class UpdateRuntimeStatisticsRunnable extends UpdateStatisticsRunnable implements Runnable {

    public UpdateRuntimeStatisticsRunnable() {
      super(UPDATE_RUNTIME_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT);
    }

    @Override
    public void runCore() throws IOException {
      Map<Domain, RuntimeStatisticsAggregator> runtimeStatisticsAggregators
          = new HashMap<Domain, RuntimeStatisticsAggregator>();
      // Compute aggregate partition runtime statistics
      for (DomainAccessor domainAccessor : domainAccessors) {
        if (domainAccessor != null) {
          runtimeStatisticsAggregators.put(domainAccessor.getHostDomain().getDomain(),
              domainAccessor.getRuntimeStatistics());
        }
      }
      // Set statistics
      Hosts.setRuntimeStatistics(host, runtimeStatisticsAggregators);
    }

    @Override
    protected void cleanup() {
      try {
        Hosts.deleteRuntimeStatistics(host);
      } catch (IOException e) {
        LOG.error("Error while deleting runtime statistics.", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void shutDown() {
    // Stop update runtime statistics
    updateRuntimeStatisticsRunnable.cancel();
    updateRuntimeStatisticsThread.interrupt();
    try {
      updateRuntimeStatisticsThread.join();
    } catch (InterruptedException e) {
      LOG.info("Interrupted while waiting for update runtime statistics thread to terminate during shutdown.");
    }
    // Shut down domain accessors
    for (DomainAccessor domainAccessor : domainAccessors) {
      if (domainAccessor != null) {
        domainAccessor.shutDown();
      }
    }
    // Shut down GET tasks
    getBulkTaskExecutor.shutdown();
    try {
      while (!getBulkTaskExecutor.awaitTermination(GET_BULK_TASK_EXECUTOR_AWAIT_TERMINATION_VALUE,
          GET_BULK_TASK_EXECUTOR_AWAIT_TERMINATION_UNIT)) {
        LOG.debug("Waiting for termination of GET BULK task executor during shutdown.");
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for termination of GET BULK task executor during shutdown.");
    }
  }
}
