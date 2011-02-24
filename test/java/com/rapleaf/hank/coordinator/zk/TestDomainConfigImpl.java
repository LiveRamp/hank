package com.rapleaf.hank.coordinator.zk;

import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.constant.ConstantStorageEngine;

public class TestDomainConfigImpl extends ZkTestCase {
  private static final String CONST_PARTITIONER = ConstantPartitioner.class.getName();
  private static final String STORAGE_ENGINE_FACTORY = ConstantStorageEngine.Factory.class.getName();
  private static final String STORAGE_ENGINE_OPTS = "---\n";

  private final String DOMAIN_PATH = getRoot() + "/test_domain_config";

  public TestDomainConfigImpl() throws Exception {
    super();
  }

  public void testLoad() throws Exception {
    // write some junk to ZK
    create(DOMAIN_PATH);
    create(DOMAIN_PATH + "/" + "num_parts", "1");
    create(DOMAIN_PATH + "/" + "storage_engine_factory_class", STORAGE_ENGINE_FACTORY);
    create(DOMAIN_PATH + "/" + "storage_engine_options", STORAGE_ENGINE_OPTS);
    create(DOMAIN_PATH + "/" + "partitioner_class", CONST_PARTITIONER);
    create(DOMAIN_PATH + "/" + "version", "1");

    // try to instantiate a DomainConfigImpl
    DomainConfigImpl dc = new DomainConfigImpl(getZk(), DOMAIN_PATH);

    // assert that we got back what we expected
    assertEquals(new Yaml().load(STORAGE_ENGINE_OPTS), dc.getStorageEngineOptions());
    assertTrue(dc.getStorageEngine() instanceof ConstantStorageEngine);
    assertEquals(1, dc.getNumParts());
    assertTrue(dc.getPartitioner() instanceof ConstantPartitioner);
    assertEquals(1, dc.getVersion());
  }
}
