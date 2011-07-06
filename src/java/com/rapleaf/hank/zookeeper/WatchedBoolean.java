package com.rapleaf.hank.zookeeper;

import org.apache.zookeeper.KeeperException;

public class WatchedBoolean extends WatchedNode<Boolean> {
  public WatchedBoolean(ZooKeeperPlus zk, String nodePath)
      throws KeeperException, InterruptedException {
    super(zk, nodePath);
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
