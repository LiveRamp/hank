package com.rapleaf.hank.part_daemon;

import java.nio.ByteBuffer;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.MockHostDomainPartition;
import com.rapleaf.hank.partitioner.MapPartitioner;
import com.rapleaf.hank.storage.Result;
import com.rapleaf.hank.storage.mock.MockReader;

public class TestDomainReaderSet extends BaseTestCase {
  public void testSetUpAndServe() throws Exception {
    PartReaderAndCounters prc[] = new PartReaderAndCounters[1];

    ByteBuffer key = ByteBuffer.wrap("key".getBytes());
    ByteBuffer nullKey = ByteBuffer.wrap("nullKey".getBytes());

    // setup DomainReaderSet
    prc[0] = new PartReaderAndCounters(new MockHostDomainPartition(0, 1, 2),
        new MockReader(null, 1, "v".getBytes()));
    // MapPartitioner maps both 'key' and 'nullkey' to prc[0]
    DomainReaderSet drs = new DomainReaderSet("domainReaderSet", prc,
        new MapPartitioner(key, 0, nullKey, 0), 2000);

    Result result = new Result();
    drs.get(key, result);
    drs.get(nullKey, result);

    assertEquals(prc[0].getRequests().get(), 2l);
    assertEquals(prc[0].getHits().get(), 1l);

    assertEquals(prc[0].getHostDomainPartition().getCount("Requests in last minute").intValue(), 0);
    assertEquals(prc[0].getHostDomainPartition().getCount("Hits in last minute").intValue(), 0);

    Thread.sleep(3000);

    assertEquals(prc[0].getRequests().get(), 0l);
    assertEquals(prc[0].getHits().get(), 0l);

    assertEquals(prc[0].getHostDomainPartition().getCount("Requests in last minute").intValue(), 2);
    assertEquals(prc[0].getHostDomainPartition().getCount("Hits in last minute").intValue(), 1);
  }
}
