package com.liveramp.hank.zookeeper;

import org.apache.zookeeper.KeeperException;

public class WatchedBoolean extends WatchedNode<Boolean> {

  public WatchedBoolean(ZooKeeperPlus zk, String nodePath, boolean waitForCreation)
      throws KeeperException, InterruptedException {
    super(zk, nodePath, waitForCreation);
  }

  public static Boolean get(ZooKeeperPlus zk, String nodePath) throws InterruptedException, KeeperException {
    return decodeValue(zk.getData(nodePath, null, null));
  }

  protected static Boolean decodeValue(byte[] data) {
    if (data == null) {
      return null;
    }
    return Boolean.parseBoolean(new String(data));
  }

  protected static byte[] encodeValue(Boolean v) {
    if (v == null) {
      return null;
    }
    return v.toString().getBytes();
  }

  @Override
  protected Boolean decode(byte[] data) {
    return decodeValue(data);
  }

  @Override
  protected byte[] encode(Boolean v) {
    return encodeValue(v);
  }
}
