package com.rapleaf.hank;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

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

  protected void create(String path) throws KeeperException,
      InterruptedException {
        create(path, (byte[])null);
      }

  protected void create(String path, String data) throws KeeperException,
      InterruptedException {
        create(path, data.getBytes());
      }

  protected void create(String path, byte[] data) throws KeeperException,
      InterruptedException {
        getZk().create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
}
