package com.rapleaf.hank.zookeeper;

import org.apache.zookeeper.KeeperException;

public class WatchedInt extends WatchedNode<Integer> {
  public WatchedInt(ZooKeeperPlus zk, String nodePath, boolean create, Integer initValue) throws KeeperException, InterruptedException {
    super(zk, nodePath, create, initValue);
  }

  public WatchedInt(ZooKeeperPlus zk, String nodePath) throws KeeperException, InterruptedException {
    super(zk, nodePath);
  }

  @Override
  protected Integer decode(byte[] data) {
    if (data == null) {
      return null;
    }
    return Integer.parseInt(new String(data));
  }

  @Override
  protected byte[] encode(Integer v) {
    if (v == null) {
      return null;
    }
    return v.toString().getBytes();
  }
}
