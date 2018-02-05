/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

public class ZooKeeperPlus extends ZooKeeper {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperPlus.class);

  private static final List<ACL> DEFAULT_ACL = Ids.OPEN_ACL_UNSAFE;
  private static final CreateMode DEFAULT_CREATE_MODE = CreateMode.PERSISTENT;

  public ZooKeeperPlus(String connectString,
                       int sessionTimeout,
                       Watcher watcher,
                       long sessionId,
                       byte[] sessionPasswd) throws IOException {
    super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd);
  }

  public ZooKeeperPlus(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
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


}
