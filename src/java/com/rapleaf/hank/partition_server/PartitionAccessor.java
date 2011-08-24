package com.rapleaf.hank.partition_server;

import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.storage.Reader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper class that stores: 1. HostDomainPartition 2. Reader: The Reader
 * associated with the HostDomainPartition 3. AtomicLong: Requests in last
 * minute counter 4. AtomicLong: Hits in last minute counter
 */
public class PartitionAccessor {
  private static final Logger LOG = Logger.getLogger(PartitionAccessor.class);
  private final HostDomainPartition partition;
  private final Reader reader;
  private final AtomicLong requests;
  private final AtomicLong hits;

  public PartitionAccessor(HostDomainPartition partition, Reader reader) {
    this.partition = partition;
    this.reader = reader;
    requests = new AtomicLong(0);
    hits = new AtomicLong(0);
    try {
      partition.setCount("Requests in last minute", 0);
      partition.setCount("Hits in last minute", 0);
    } catch (IOException e) {
      LOG.error("Counldn't set counter", e);
    }
  }

  public HostDomainPartition getHostDomainPartition() {
    return partition;
  }

  public Reader getReader() {
    return reader;
  }

  public AtomicLong getRequests() {
    return requests;
  }

  public AtomicLong getHits() {
    return hits;
  }

  public void updateCounters() throws IOException {
    updateCounter(requests, "Requests in last minute");
    updateCounter(hits, "Hits in last minute");
  }

  private void updateCounter(AtomicLong counter, String counterName)
      throws IOException {
    long count = counter.get();
    partition.setCount(counterName, count);
    counter.addAndGet(-count);
  }
}
