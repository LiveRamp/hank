package com.rapleaf.hank.partition_server;

import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper class that stores: 1. HostDomainPartition 2. Reader: The Reader
 * associated with the HostDomainPartition 3. AtomicLong: Requests in last
 * minute counter 4. AtomicLong: Hits in last minute counter
 */
public class PartitionAccessor {
  private static final HankResponse NOT_FOUND = HankResponse.not_found(true);
  private static final Logger LOG = Logger.getLogger(PartitionAccessor.class);
  protected static final String KEYS_REQUESTED_COUNTER_NAME = "Keys requested in last minute";
  protected static final String KEYS_FOUND_COUNTER_NAME = "Keys found in last minute";

  private final HostDomainPartition partition;
  private final Reader reader;
  private final AtomicLong requests;
  private final AtomicLong hits;

  public PartitionAccessor(HostDomainPartition partition, Reader reader) {
    if (reader == null) {
      throw new IllegalArgumentException("Reader may not be null!");
    }
    this.partition = partition;
    this.reader = reader;
    requests = new AtomicLong(0);
    hits = new AtomicLong(0);
    try {
      partition.setCount("Requests in last minute", 0);
      partition.setCount("Hits in last minute", 0);
    } catch (IOException e) {
      LOG.error("Couldn't set counter", e);
    }
  }

  public HostDomainPartition getHostDomainPartition() {
    return partition;
  }

  public HankResponse get(ByteBuffer key) throws IOException {
    // Increment requests counter
    LOG.trace("Partition GET");
    requests.incrementAndGet();
    Result result = new Result();
    reader.get(key, result);
    if (result.isFound()) {
      // Increment hits counter
      hits.incrementAndGet();
      return HankResponse.value(result.getBuffer());
    } else {
      return NOT_FOUND;
    }
  }

  public long getRequestsCount() {
    return requests.get();
  }

  public long getHitsCount() {
    return hits.get();
  }

  public void updateGlobalCounters() throws IOException {
    updateGlobalCounter(requests, KEYS_REQUESTED_COUNTER_NAME);
    updateGlobalCounter(hits, KEYS_FOUND_COUNTER_NAME);
  }

  private void updateGlobalCounter(AtomicLong counter, String counterName)
      throws IOException {
    long count = counter.get();
    partition.setCount(counterName, count);
    counter.addAndGet(-count);
  }
}
