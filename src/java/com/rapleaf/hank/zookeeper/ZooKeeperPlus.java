package com.rapleaf.hank.zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperPlus extends ZooKeeper {
  public ZooKeeperPlus(String connectString,
      int sessionTimeout,
      Watcher watcher,
      long sessionId,
      byte[] sessionPasswd) throws IOException {
    super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd);
  }

  public ZooKeeperPlus(String connectString, int sessionTimeout, Watcher watcher)
      throws IOException {
    super(connectString, sessionTimeout, watcher);
  }

  public Integer getIntOrNull(String path) throws KeeperException, InterruptedException {
    if (exists(path, false) == null) {
      return null;
    } else {
      return Integer.parseInt(new String(getData(path, false, new Stat())));
    }
  }

  public int getInt(String path) throws KeeperException, InterruptedException {
    return Integer.parseInt(new String(getData(path, false, new Stat())));
  }

  public String getString(String path) throws KeeperException, InterruptedException {
    try {
      byte[] data = getData(path, false, null);
      if (data == null) {
        return null;
      }
      return new String(data, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public void setInt(String path, int nextVersion) throws KeeperException, InterruptedException {
    setData(path, ("" + nextVersion).getBytes(), -1);
  }

  public void setString(String path, String value) throws KeeperException, InterruptedException {
    setData(path, value.getBytes(), -1);
  }

  public void deleteIfExists(String path) throws KeeperException, InterruptedException {
    if (exists(path, false) != null) {
      delete(path, -1);
    }
  }

  public void setOrCreate(String path, int value, CreateMode createMode) throws KeeperException, InterruptedException {
    setOrCreate(path, "" + value, createMode);
  }

  public void setOrCreate(String path, String value, CreateMode createMode) throws KeeperException, InterruptedException {
    if (exists(path, false) == null) {
      create(path, value.getBytes(), Ids.OPEN_ACL_UNSAFE, createMode);
    } else {
      setData(path, value.getBytes(), -1);
    }
  }

  public void deleteNodeRecursively(String path) throws InterruptedException, KeeperException {
    try {
      delete(path, -1);
    } catch (KeeperException.NotEmptyException e) {
      List<String> children = getChildren(path, null);
      for (String child : children) {
        deleteNodeRecursively(path + "/" + child);
      }
      delete(path, -1);
    } catch (KeeperException.NoNodeException e) {
      // Silently return if the node has already been deleted.
      return;
    }
  }

  public void create(String path, int v, CreateMode createMode) throws KeeperException, InterruptedException {
    create(path, (""+v).getBytes(), Ids.OPEN_ACL_UNSAFE, createMode);
  }
}
