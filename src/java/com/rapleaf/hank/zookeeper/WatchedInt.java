package com.rapleaf.hank.zookeeper;

import org.apache.zookeeper.KeeperException;

public class WatchedInt extends WatchedNode<Integer> {
  public WatchedInt(ZooKeeperPlus zk, String nodePath)
      throws KeeperException, InterruptedException {
    super(zk, nodePath);
  }

  @Override
  protected Integer decode(byte[] data) {
    if (data == null) {
      return null;
    }
    return Integer.parseInt(new String(data));
  }
}
