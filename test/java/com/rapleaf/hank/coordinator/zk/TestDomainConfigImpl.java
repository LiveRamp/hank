package com.rapleaf.hank.coordinator.zk;

import junit.framework.TestCase;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.constant.ConstantStorageEngine;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class TestDomainConfigImpl extends TestCase {
  private static final String DOMAIN_PATH = "/arbitrary_root/test_domain_config";
  private static final String CONST_PARTITIONER = "com.rapleaf.hank.partitioner.ConstantPartitioner";
  private static final String STORAGE_ENGINE_FACTORY = "com.rapleaf.hank.storage.constant.ConstantStorageEngine$Factory";
  private static final String STORAGE_ENGINE_OPTS = "---\n";

  public void testLoad() throws Exception {
    // write some junk to ZK
    ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 1000000, null);
    ZooKeeperUtils.deleteNodeRecursively(zk, DOMAIN_PATH);
    ZooKeeperUtils.createNodeRecursively(zk, DOMAIN_PATH);
    node(zk, "num_parts", "1");
    node(zk, "storage_engine_factory_class", STORAGE_ENGINE_FACTORY);
    node(zk, "storage_engine_options", STORAGE_ENGINE_OPTS);
    node(zk, "partitioner_class", CONST_PARTITIONER);
    node(zk, "version", "1");

    // try to instantiate a DomainConfigImpl
    DomainConfigImpl dc = new DomainConfigImpl(zk, DOMAIN_PATH);

    // assert that we got back what we expected
    assertEquals(new Yaml().load(STORAGE_ENGINE_OPTS), dc.getStorageEngineOptions());
    assertTrue(dc.getStorageEngine() instanceof ConstantStorageEngine);
    assertEquals(1, dc.getNumParts());
    assertTrue(dc.getPartitioner() instanceof ConstantPartitioner);
    assertEquals(1, dc.getVersion());
  }

  private static void node(ZooKeeper zk, String path, String data) throws Exception {
    zk.create(DOMAIN_PATH + "/" + path, data.getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }
}
