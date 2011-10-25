package com.rapleaf.hank.partition_server;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.MockHostDomainPartition;
import com.rapleaf.hank.partitioner.MapPartitioner;
import com.rapleaf.hank.storage.ReaderResult;
import com.rapleaf.hank.storage.mock.MockReader;

import java.nio.ByteBuffer;

public class TestDomainAccessor extends BaseTestCase {
  public void testSetUpAndServe() throws Exception {
    PartitionAccessor partitionAccessors[] = new PartitionAccessor[1];

    ByteBuffer key = ByteBuffer.wrap("key".getBytes());
    ByteBuffer nullKey = ByteBuffer.wrap("nullKey".getBytes());

    // setup DomainAccessor
    partitionAccessors[0] = new PartitionAccessor(new MockHostDomainPartition(0, 1, 2),
        new MockReader(null, 1, "v".getBytes(), null));
    // MapPartitioner maps both 'key' and 'nullkey' to partitionAccessors[0]
    DomainAccessor drs = new DomainAccessor("domainReaderSet", partitionAccessors,
        new MapPartitioner(key, 0, nullKey, 0), 2000);

    drs.get(key, new ReaderResult());
    drs.get(nullKey, new ReaderResult());

    assertEquals(2l, partitionAccessors[0].getRequestsCount());
    assertEquals(1l, partitionAccessors[0].getHitsCount());

    assertEquals(0, partitionAccessors[0].getHostDomainPartition().getCount(PartitionAccessor.KEYS_REQUESTED_COUNTER_NAME).intValue());
    assertEquals(0, partitionAccessors[0].getHostDomainPartition().getCount(PartitionAccessor.KEYS_FOUND_COUNTER_NAME).intValue());

    Thread.sleep(3000);

    assertEquals(0l, partitionAccessors[0].getRequestsCount());
    assertEquals(0l, partitionAccessors[0].getHitsCount());

    assertEquals(2, partitionAccessors[0].getHostDomainPartition().getCount(PartitionAccessor.KEYS_REQUESTED_COUNTER_NAME).intValue());
    assertEquals(1, partitionAccessors[0].getHostDomainPartition().getCount(PartitionAccessor.KEYS_FOUND_COUNTER_NAME).intValue());
  }
}
