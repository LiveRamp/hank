/**
 *  Copyright 2011 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.hank.zookeeper;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperPlus {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperPlus.class);

  private static final List<ACL> DEFAULT_ACL = Ids.OPEN_ACL_UNSAFE;
  private static final CreateMode DEFAULT_CREATE_MODE = CreateMode.PERSISTENT;

  private ZKPCore conn;

  private final String reconnectString;
  private final int sessionTimeout;
  private final Watcher watcher;
  private final Thread reconnectWatcher;

  public ZooKeeperPlus(String reconnectString, int sessionTimeout, Watcher watcher){
    this.reconnectString = reconnectString;
    this.sessionTimeout = sessionTimeout;
    this.watcher = watcher;

    this.reconnectWatcher = new Thread(new ReconnectWatcher());
    this.reconnectWatcher.setDaemon(true);
    this.reconnectWatcher.start();
  }

  public synchronized void reconnect() throws IOException {
    conn = new ZKPCore(reconnectString, sessionTimeout, watcher);
  }

  public class ReconnectWatcher implements Runnable {

    private static final long POLL_INTERVAL = 30000;  // 30s

    @Override
    public void run() {

      try {

        while(true) {
          Thread.sleep(POLL_INTERVAL);
          //  don't start reconnect until we explicitly connect for the first time
          if (conn != null && conn.getState() != ZooKeeper.States.CONNECTED) {
            LOG.info("ZooKeeper is not connected.  Closing connection");
            close();
            try {
              LOG.info("Reconnecting");
              reconnect();
              LOG.info("Reconnected");
            } catch (IOException e) {
              LOG.error("Error reconnecting to ZooKeeper", e);
            }
          }else{
            LOG.info("ZooKeeper is connected, sleeping for "+POLL_INTERVAL+"ms");
          }
        }

      } catch (InterruptedException e) {
        LOG.error("Interrupting watcher sleep", e);
      }

    }

  }

  public void create(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {
    conn.create(path, data, createMode);
  }

  public void create(String path, byte[] data) throws KeeperException, InterruptedException {
    conn.create(path, data);
  }

  public String create(final String path, byte data[], List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException{
    return conn.create(path, data, acl, createMode);
  }

  public void create(final String path, byte data[], List<ACL> acl, CreateMode createMode,  AsyncCallback.StringCallback cb, Object ctx){
    conn.create(path, data, acl, createMode, cb, ctx);

  }


  public void createLong(String path, long value) throws KeeperException, InterruptedException {
    conn.createLong(path, value);
  }

  public void createInt(String path, int value) throws KeeperException, InterruptedException {
    conn.createInt(path, value);
  }

  public Integer getIntOrNull(String path) throws KeeperException, InterruptedException {
    return conn.getIntOrNull(path);
  }

  public int getInt(String path) throws KeeperException, InterruptedException {
    return conn.getInt(path);
  }

  public void setString(String path, String value) throws KeeperException, InterruptedException {
    conn.setString(path, value);
  }

  public void setInt(String path, int nextVersion) throws KeeperException, InterruptedException {
    conn.setInt(path, nextVersion);
  }

  public long getLong(String path) throws KeeperException, InterruptedException {
    return conn.getLong(path);
  }

  public Long getLongOrNull(String path) throws KeeperException, InterruptedException {
    return conn.getLongOrNull(path);
  }

  public String getString(String path) throws KeeperException, InterruptedException {
    return conn.getString(path);
  }

  public void deleteIfExists(String path) throws KeeperException, InterruptedException {
    conn.deleteIfExists(path);
  }

  public void setOrCreate(String path, int value, CreateMode createMode) throws KeeperException, InterruptedException {
    conn.setOrCreate(path, value, createMode);
  }

  public void setOrCreate(String path, long value, CreateMode createMode) throws KeeperException, InterruptedException {
    conn.setOrCreate(path, value, createMode);
  }

  public void setOrCreate(String path, String value, CreateMode createMode) throws KeeperException, InterruptedException {
    conn.setOrCreate(path, value, createMode);
  }

  public void ensureCreated(String path, byte[] value) throws InterruptedException, KeeperException {
    conn.ensureCreated(path, value);
  }

  public void ensureCreated(String path, byte[] value, CreateMode createMode) throws InterruptedException, KeeperException {
    conn.ensureCreated(path, value, createMode);
  }

  public void deleteNodeRecursively(String path) throws InterruptedException, KeeperException {
    conn.deleteNodeRecursively(path);
  }

  // Get not hidden children (children that do not start with a period)
  public List<String> getChildrenNotHidden(String path, boolean watch) throws InterruptedException, KeeperException {
    return conn.getChildrenNotHidden(path, watch);
  }

  public List<String> getChildrenNotHidden(String path, Watcher watcher) throws InterruptedException, KeeperException {
    return conn.getChildrenNotHidden(path, watcher);
  }

  public int getSessionTimeout() {
    return conn.getSessionTimeout();
  }

  public List<String> getChildren(final String path, Watcher watcher) throws KeeperException, InterruptedException {
    return conn.getChildren(path, watcher);
  }

  public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
    return conn.exists(path, watch);
  }

  public void exists(final String path, Watcher watcher, AsyncCallback.StatCallback cb, Object ctx) {
    conn.exists(path, watcher, cb, ctx);
  }

  public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx) {
    conn.exists(path, watch, cb, ctx);
  }

  public Stat exists(final String path, Watcher watcher) throws KeeperException, InterruptedException {
    return conn.exists(path, watcher);
  }


  public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
    return conn.getChildrenNotHidden(path, watch);
  }

  public void getChildren(final String path, Watcher watcher, AsyncCallback.ChildrenCallback cb, Object ctx) {
    conn.getChildren(path, watcher, cb, ctx);
  }

  public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx) {
    conn.getChildren(path, watch, cb, ctx);
  }

  public List<String> getChildren(final String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
    return conn.getChildren(path, watcher, stat);
  }

  public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
    return conn.getChildren(path, watch, stat);
  }

  public void getChildren(final String path, Watcher watcher, AsyncCallback.Children2Callback cb, Object ctx) {
    conn.getChildren(path, watcher, cb, ctx);
  }

  public void getChildren(String path, boolean watch, AsyncCallback.Children2Callback cb, Object ctx) {
    conn.getChildren(path, watch, cb, ctx);
  }

  public void delete(final String path, int version) throws InterruptedException, KeeperException {
    conn.delete(path, version);
  }

  public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
    conn.getData(path, watch, cb, ctx);
  }

  public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
    return conn.getData(path, watch, stat);
  }

  public void getData(final String path, Watcher watcher, AsyncCallback.DataCallback cb, Object ctx) {
    conn.getData(path, watcher, cb, ctx);
  }

  public byte[] getData(final String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
    return conn.getData(path, watcher, stat);
  }

  public Stat setData(final String path, byte data[], int version) throws KeeperException, InterruptedException {
    return conn.setData(path, data, version);
  }

  public void setData(final String path, byte data[], int version, AsyncCallback.StatCallback cb, Object ctx){
    conn.setData(path, data, version, cb, ctx);
  }

  public synchronized void close() throws InterruptedException {
    conn.close();
    reconnectWatcher.interrupt();
  }

  public ZooKeeper.States getState() {
    return conn.getState();
  }

  private class ZKPCore extends ZooKeeper {

    private ZKPCore(String connectString,
                    int sessionTimeout,
                    Watcher watcher,
                    long sessionId,
                    byte[] sessionPasswd) throws IOException {
      super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd);
    }

    private ZKPCore(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
      super(connectString, sessionTimeout, watcher);
    }

    public void create(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {
      create(path, data, DEFAULT_ACL, createMode);
    }

    public void create(String path, byte[] data) throws KeeperException, InterruptedException {
      create(path, data, DEFAULT_ACL, DEFAULT_CREATE_MODE);
    }

    public void createLong(String path, long value) throws KeeperException, InterruptedException {
      create(path, (Long.toString(value)).getBytes(), DEFAULT_ACL, DEFAULT_CREATE_MODE);
    }

    public void createInt(String path, int value) throws KeeperException, InterruptedException {
      create(path, (Integer.toString(value)).getBytes(), DEFAULT_ACL, DEFAULT_CREATE_MODE);
    }

    public Integer getIntOrNull(String path) throws KeeperException, InterruptedException {
      Long lvalue = getLongOrNull(path);
      if (lvalue == null) {
        return null;
      }
      return lvalue.intValue();
    }

    public int getInt(String path) throws KeeperException, InterruptedException {
      return Integer.parseInt(new String(getData(path, false, new Stat())));
    }

    public void setString(String path, String value) throws KeeperException, InterruptedException {
      setData(path, value.getBytes(), -1);
    }

    public void setInt(String path, int nextVersion) throws KeeperException, InterruptedException {
      setData(path, (Integer.toString(nextVersion)).getBytes(), -1);
    }

    public long getLong(String path) throws KeeperException, InterruptedException {
      return Long.parseLong(new String(getData(path, false, new Stat())));
    }

    public Long getLongOrNull(String path) throws KeeperException, InterruptedException {
      if (exists(path, false) == null) {
        return null;
      } else {
        return Long.parseLong(new String(getData(path, false, new Stat())));
      }
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

    public void deleteIfExists(String path) throws KeeperException, InterruptedException {
      if (exists(path, false) != null) {
        delete(path, -1);
      }
    }

    public void setOrCreate(String path, int value, CreateMode createMode) throws KeeperException, InterruptedException {
      setOrCreate(path, Integer.toString(value), createMode);
    }

    public void setOrCreate(String path, long value, CreateMode createMode) throws KeeperException, InterruptedException {
      setOrCreate(path, Long.toString(value), createMode);
    }

    public void setOrCreate(String path, String value, CreateMode createMode) throws KeeperException, InterruptedException {
      if (exists(path, false) == null) {
        create(path, value.getBytes(), DEFAULT_ACL, createMode);
      } else {
        setData(path, value.getBytes(), -1);
      }
    }

    public void ensureCreated(String path, byte[] value) throws InterruptedException, KeeperException {
      ensureCreated(path, value, DEFAULT_CREATE_MODE);
    }

    public void ensureCreated(String path, byte[] value, CreateMode createMode) throws InterruptedException, KeeperException {
      if (!path.isEmpty() && exists(path, false) == null) {
        ensureCreated(new File(path).getParent(), null, createMode);
        create(path, value, DEFAULT_ACL, createMode);
        NodeCreationBarrier.block(ZooKeeperPlus.this, path);
      }
    }

    public void deleteNodeRecursively(String path) throws InterruptedException, KeeperException {
      try {
        delete(path, -1);
      } catch (KeeperException.NotEmptyException e) {
        List<String> children = getChildren(path, null);
        for (String child : children) {
          deleteNodeRecursively(ZkPath.append(path, child));
        }
        delete(path, -1);
      } catch (KeeperException.NoNodeException e) {
        // Silently return if the node has already been deleted.
        return;
      }
    }

    // Get not hidden children (children that do not start with a period)
    public List<String> getChildrenNotHidden(String path, boolean watch) throws InterruptedException, KeeperException {
      return ZkPath.filterOutHiddenPaths(super.getChildren(path, watch));
    }

    public List<String> getChildrenNotHidden(String path, Watcher watcher) throws InterruptedException, KeeperException {
      return ZkPath.filterOutHiddenPaths(super.getChildren(path, watcher));
    }
  }
}
