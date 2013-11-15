package com.liveramp.hank.partition_server;

import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHostDomain;
import com.liveramp.hank.test.coordinator.MockHostDomainPartition;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.test.partitioner.MapPartitioner;
import com.liveramp.hank.storage.ReaderResult;
import com.liveramp.hank.storage.mock.MockReader;
import org.junit.Test;

import java.nio.ByteBuffer;

public class TestDomainAccessor extends BaseTestCase {
  @Test
  public void testSetUpAndServe() throws Exception {
    PartitionAccessor partitionAccessors[] = new PartitionAccessor[1];

    ByteBuffer key = ByteBuffer.wrap("key".getBytes());
    ByteBuffer nullKey = ByteBuffer.wrap("nullKey".getBytes());

    // setup DomainAccessor
    partitionAccessors[0] = new PartitionAccessor(new MockHostDomainPartition(0, 1),
        new MockReader(null, 1, "v".getBytes(), null));
    // MapPartitioner maps both 'key' and 'nullkey' to partitionAccessors[0]
    DomainAccessor drs = new DomainAccessor(new MockHostDomain(new MockDomain("domain")), partitionAccessors,
        new MapPartitioner(key, 0, nullKey, 0), 0);

    drs.get(key, new ReaderResult());
    drs.get(nullKey, new ReaderResult());
  }
}
