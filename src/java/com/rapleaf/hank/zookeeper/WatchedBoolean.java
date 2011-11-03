package com.rapleaf.hank.zookeeper;

import org.apache.zookeeper.KeeperException;

public class WatchedBoolean extends WatchedNode<Boolean> {
  public WatchedBoolean(ZooKeeperPlus zk, String nodePath, Boolean initialValue)
      throws KeeperException, InterruptedException {
    super(zk, nodePath, initialValue);
  }

  public WatchedBoolean(ZooKeeperPlus zk, String nodePath, boolean waitForCreation)
      throws KeeperException, InterruptedException {
    super(zk, nodePath, waitForCreation);
  }

  @Override
  protected Boolean decode(byte[] data) {
    if (data == null) {
      return null;
    }
    return Boolean.parseBoolean(new String(data));
  }

  @Override
  protected byte[] encode(Boolean v) {
    if (v == null) {
      return null;
    }
    return v.toString().getBytes();
  }
}
