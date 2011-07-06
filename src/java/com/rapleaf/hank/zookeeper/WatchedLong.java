package com.rapleaf.hank.zookeeper;

import org.apache.zookeeper.KeeperException;

public class WatchedLong extends WatchedNode<Long> {
  protected WatchedLong(ZooKeeperPlus zk, String nodePath)
      throws KeeperException, InterruptedException {
    super(zk, nodePath);
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
