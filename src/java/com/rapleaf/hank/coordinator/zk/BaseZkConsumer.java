package com.rapleaf.hank.coordinator.zk;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

abstract class BaseZkConsumer {
  protected final ZooKeeper zk;

  protected BaseZkConsumer(ZooKeeper zk) {
    this.zk = zk;
  }

  protected Integer getIntOrNull(String path) throws KeeperException, InterruptedException {
    if (zk.exists(path, false) == null) {
      return null;
    } else {
      return Integer.parseInt(new String(zk.getData(path, false, new Stat())));
    }
  }

  protected int getInt(String path) throws KeeperException, InterruptedException {
    return Integer.parseInt(new String(zk.getData(path, false, new Stat())));
  }

  protected String getString(String path) throws KeeperException, InterruptedException {
    try {
      byte[] data = zk.getData(path, false, null);
      if (data == null) {
        return null;
      }
      return new String(data, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  protected void setInt(String path, int nextVersion) throws KeeperException,
      InterruptedException {
        zk.setData(path, ("" + nextVersion).getBytes(), -1);
      }

  public static void deleteNodeRecursively(ZooKeeper zk, String path) throws InterruptedException, KeeperException {
    try {
      zk.delete(path, -1);
    } catch (KeeperException.NotEmptyException e) {
      List<String> children = zk.getChildren(path, null);
      for (String child : children) {
        deleteNodeRecursively(zk, path + "/" + child);
      }
      zk.delete(path, -1);
    } catch (KeeperException.NoNodeException e) {
      // Silently return if the node has already been deleted.
      return;
    }
  }
}
