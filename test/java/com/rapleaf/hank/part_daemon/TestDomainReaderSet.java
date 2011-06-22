package com.rapleaf.hank.part_daemon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.coordinator.AbstractHostDomain;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.coordinator.MockDomainGroup;
import com.rapleaf.hank.coordinator.MockDomainGroupVersion;
import com.rapleaf.hank.coordinator.MockDomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.MockHost;
import com.rapleaf.hank.coordinator.MockHostDomainPartition;
import com.rapleaf.hank.coordinator.MockRing;
import com.rapleaf.hank.coordinator.MockRingGroup;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.generated.HankExceptions;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.partitioner.MapPartitioner;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.Result;
import com.rapleaf.hank.storage.mock.MockReader;
import com.rapleaf.hank.storage.mock.MockStorageEngine;
import com.sun.org.apache.bcel.internal.generic.NEW;

public class TestDomainReaderSet extends BaseTestCase {
  
  PartReaderAndCounters prc[] = new PartReaderAndCounters[1];
  DomainReaderSet drs;
  
  public void testSetUpAndServe() throws IOException, InterruptedException {
    
    int timeout = 2000;
    
    ByteBuffer key = ByteBuffer.wrap("key".getBytes());
    ByteBuffer nullKey = ByteBuffer.wrap("nullKey".getBytes());
    
    // setup DomainReaderSet
    prc[0] = new PartReaderAndCounters(new MockHostDomainPartition(0, 1, 2), new MockReader(null, 1, "v".getBytes()));
    try {
      // MapPartitioner maps both 'key' and 'nullkey' to prc[0]
      drs = new DomainReaderSet("domainReaderSet", prc, new MapPartitioner(key, 0, nullKey, 0), timeout);
    } catch (IOException e) {}
    
    Result result = new Result();
    drs.get(key, result);
    drs.get(nullKey, result);
    
    assertEquals(prc[0].getRequests().get(), 2l);
    assertEquals(prc[0].getHits().get(), 1l);
    
    assertEquals(prc[0].getHostDomainPartition().getCount("Requests").intValue(), 0);
    assertEquals(prc[0].getHostDomainPartition().getCount("Hits").intValue(), 0);
    
    Thread.sleep(timeout + 500);
    
    assertEquals(prc[0].getRequests().get(), 0l);
    assertEquals(prc[0].getHits().get(), 0l);
    
    assertEquals(prc[0].getHostDomainPartition().getCount("Requests").intValue(), 2);
    assertEquals(prc[0].getHostDomainPartition().getCount("Hits").intValue(), 1);
    
  }
}
