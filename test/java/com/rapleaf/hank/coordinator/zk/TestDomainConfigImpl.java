package com.rapleaf.hank.coordinator.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.constant.ConstantStorageEngine;

// TODO: make this work like the other zktests
public class TestDomainConfigImpl extends ZkTestCase {
  private static final String CONST_PARTITIONER = "com.rapleaf.hank.partitioner.ConstantPartitioner";
  private static final String STORAGE_ENGINE_FACTORY = "com.rapleaf.hank.storage.constant.ConstantStorageEngine$Factory";
  private static final String STORAGE_ENGINE_OPTS = "---\n";

  private final String DOMAIN_PATH = getRoot() + "/test_domain_config";

  public TestDomainConfigImpl() throws Exception {
    super();
  }

  public void testLoad() throws Exception {
    // write some junk to ZK
    getZk().create(DOMAIN_PATH, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    node(getZk(), "num_parts", "1");
    node(getZk(), "storage_engine_factory_class", STORAGE_ENGINE_FACTORY);
    node(getZk(), "storage_engine_options", STORAGE_ENGINE_OPTS);
    node(getZk(), "partitioner_class", CONST_PARTITIONER);
    node(getZk(), "version", "1");

    // try to instantiate a DomainConfigImpl
    DomainConfigImpl dc = new DomainConfigImpl(getZk(), DOMAIN_PATH);

    // assert that we got back what we expected
    assertEquals(new Yaml().load(STORAGE_ENGINE_OPTS), dc.getStorageEngineOptions());
    assertTrue(dc.getStorageEngine() instanceof ConstantStorageEngine);
    assertEquals(1, dc.getNumParts());
    assertTrue(dc.getPartitioner() instanceof ConstantPartitioner);
    assertEquals(1, dc.getVersion());
  }

  private void node(ZooKeeper zk, String path, String data) throws Exception {
    zk.create(DOMAIN_PATH + "/" + path, data.getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }
}
