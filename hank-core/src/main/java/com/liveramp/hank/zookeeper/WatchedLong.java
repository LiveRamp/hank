package com.liveramp.hank.zookeeper;

import org.apache.zookeeper.KeeperException;

public class WatchedLong extends WatchedNode<Long> {

  public WatchedLong(ZooKeeperPlus zk, String nodePath, boolean waitForCreation)
      throws KeeperException, InterruptedException {
    super(zk, nodePath, waitForCreation);
  }

  public WatchedLong(ZooKeeperPlus zk, String nodePath)
      throws KeeperException, InterruptedException {
    this(zk, nodePath, false);
  }

  @Override
  protected Long decode(byte[] data) {
    if (data == null) {
      return null;
    }
    return Long.parseLong(new String(data));
  }

  @Override
  protected byte[] encode(Long v) {
    if (v == null) {
      return null;
    }
    return v.toString().getBytes();
  }
}
