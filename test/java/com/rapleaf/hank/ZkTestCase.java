package com.rapleaf.hank;

import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.util.ZooKeeperUtils;

public class ZkTestCase extends BaseTestCase {
  private final String zkRoot;
  private final ZooKeeper zk;

  public ZkTestCase() throws Exception {
    super();
    zkRoot = "/" + getClass().getSimpleName();
    zk = new ZooKeeper("127.0.0.1:2181", 1000000, null);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ZooKeeperUtils.deleteNodeRecursively(zk, zkRoot);
    ZooKeeperUtils.createNodeRecursively(zk, zkRoot);
  }

  public String getRoot() {
    return zkRoot;
  }

  public ZooKeeper getZk() {
    return zk;
  }
}
