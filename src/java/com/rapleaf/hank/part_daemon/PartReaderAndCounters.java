package com.rapleaf.hank.part_daemon;

import java.io.IOException;
import java.util.Set;

import org.apache.log4j.Logger;

import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.storage.Reader;

/**
 * Wrapper class that stores:
 * 1. HostDomainPartition
 * 2. Reader: The Reader associated with the HostDomainPartition
 * 3. CounterCache: Requests counter
 * 4. CounterCache: Hits counter
 */
public class PartReaderAndCounters {
  
  private static final Logger LOG = Logger.getLogger(DomainReaderSet.class);
  private final HostDomainPartition part;
  private final Reader reader;
  private final CounterCache requests;
  private final CounterCache hits;
  
  public PartReaderAndCounters (HostDomainPartition part, Reader reader) {
    this.part = part;
    this.reader = reader;
    requests = new CounterCache("Requests");
    hits = new CounterCache("Hits");
    try {
      part.setCount(requests.getName(), 0);
      part.setCount(hits.getName(), 0);
    } catch (IOException e) {
      LOG.error("Counldn't set counter", e);
    }
  }
  
  public HostDomainPartition getHostDomainPartition(){
    return part;
  }
  
  public Reader getReader() {
    return reader;
  }
  
  public CounterCache getRequests() {
    return requests;
  }
  
  public CounterCache getHits() {
    return hits;
  }
  
  public void updateCounters() throws IOException {
    updateCounter(requests);
    updateCounter(hits);
  }
  
  private void updateCounter(CounterCache counter) throws IOException {
    long count = counter.getCount().get();
    String counterName = counter.getName();
    part.setCount(counterName, part.getCount(counterName) + count);
    counter.getCount().set(0);
  }
  
}
