package com.rapleaf.hank.coordinator.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.config.DomainGroupConfigVersion;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.constant.ConstantStorageEngine;

public class TestDomainGroupConfigImpl extends ZkTestCase {
  private final String dg_root = getRoot() + "/myDomainGroup";
  private final String domains_root = getRoot() + "/domains";

  public TestDomainGroupConfigImpl() throws Exception {
    super();
  }

  public void testLoad() throws Exception {
    create(domains_root);
    create(domains_root + "/domain0");
    create(domains_root + "/domain0/num_parts", "1");
    create(domains_root + "/domain0/version", "1");
    create(domains_root + "/domain0/storage_engine_options", "---");
    create(domains_root + "/domain0/storage_engine_factory_class", ConstantStorageEngine.Factory.class.getName());
    create(domains_root + "/domain0/partitioner_class", ConstantPartitioner.class.getName());
    create(domains_root + "/domain1");
    create(domains_root + "/domain1/num_parts", "1");
    create(domains_root + "/domain1/version", "1");
    create(domains_root + "/domain1/storage_engine_options", "---");
    create(domains_root + "/domain1/storage_engine_factory_class", ConstantStorageEngine.Factory.class.getName());
    create(domains_root + "/domain1/partitioner_class", ConstantPartitioner.class.getName());
    create(dg_root);
    create(dg_root + "/domains");
    create(dg_root + "/domains/0", domains_root + "/domain0");
    create(dg_root + "/domains/1", domains_root + "/domain1");
    create(dg_root + "/versions");
    create(dg_root + "/versions/1");
    create(dg_root + "/versions/1/domain0", "1");
    create(dg_root + "/versions/1/domain1", "1");

    DomainGroupConfigImpl dgc = new DomainGroupConfigImpl(getZk(), dg_root);

    assertEquals(1, dgc.getVersions().size());
    assertEquals(1, ((DomainGroupConfigVersion)dgc.getVersions().toArray()[0]).getVersionNumber());
    assertEquals(1, dgc.getLatestVersion().getVersionNumber());
    assertEquals(0, dgc.getDomainId("domain0"));
    assertEquals(1, dgc.getDomainId("domain1"));
    assertEquals("domain0", dgc.getDomainConfig(0).getName());
    assertEquals("domain1", dgc.getDomainConfig(1).getName());
  }

  private void create(String path) throws KeeperException, InterruptedException {
    create(path, (byte[])null);
  }

  private void create(String path, String data) throws KeeperException, InterruptedException {
    create(path, data.getBytes());
  }

  private void create(String path, byte[] data) throws KeeperException, InterruptedException {
    getZk().create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }
}
