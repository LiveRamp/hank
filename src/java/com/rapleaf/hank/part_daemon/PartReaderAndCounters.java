package com.rapleaf.hank.part_daemon;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.storage.Reader;

/**
 * Wrapper class that stores: 1. HostDomainPartition 2. Reader: The Reader
 * associated with the HostDomainPartition 3. AtomicLong: Requests in last
 * minute counter 4. AtomicLong: Hits in last minute counter
 */
public class PartReaderAndCounters {
  private static final Logger LOG = Logger.getLogger(DomainReaderSet.class);
  private final HostDomainPartition part;
  private final Reader reader;
  private final AtomicLong requests;
  private final AtomicLong hits;

  public PartReaderAndCounters(HostDomainPartition part, Reader reader) {
    this.part = part;
    this.reader = reader;
    requests = new AtomicLong(0);
    hits = new AtomicLong(0);
    try {
      part.setCount("Requests in last minute", 0);
      part.setCount("Hits in last minute", 0);
    } catch (IOException e) {
      LOG.error("Counldn't set counter", e);
    }
  }

  public HostDomainPartition getHostDomainPartition() {
    return part;
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
    part.setCount(counterName, count);
    counter.addAndGet(-count);
  }
}
