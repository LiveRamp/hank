/**
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.util;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.exception.DataNotFoundException;

public final class ZooKeeperUtils {
  private static final Logger LOG = Logger.getLogger(ZooKeeperUtils.class);

  private ZooKeeperUtils() {}

  public static final String ROOT = "/tiamat";
  public static final String DOMAIN_ROOT = ROOT + "/domains";
  public static final String DOMAIN_GROUP_ROOT = ROOT + "/domain_groups";
  public static final String RING_GROUP_ROOT = ROOT + "/ring_groups";

  private static final String RING_PATH_FORMAT = RING_GROUP_ROOT
      + "/%s/ring-%03d";

  public static String getRingPath(String ringGroupName, int ringNumber) {
    return String.format(RING_PATH_FORMAT, ringGroupName, ringNumber);
  }

  private static final String HOST_PATH_FORMAT = RING_PATH_FORMAT + "/hosts/%s";

  public static String getHostPath(String ringGroupName, int ringNumber,
      PartDaemonAddress hostName) {
    return String.format(HOST_PATH_FORMAT, ringGroupName, ringNumber, hostName);
  }

  private static final String DAEMON_STATUS_FORMAT = "%s/%s/status";

  public static String getDaemonStatusPath(String ringGroupName,
      int ringNumber, PartDaemonAddress hostName, DaemonType type) {
    return String.format(DAEMON_STATUS_FORMAT, getHostPath(ringGroupName,
        ringNumber, hostName), type.name);
  }

  private static final String DOMAIN_PATH_FORMAT = DOMAIN_ROOT + "/%s";

  public static String getDomainPath(String domainName) {
    return String.format(DOMAIN_PATH_FORMAT, domainName);
  }

  private static final String DOMAIN_GROUP_PATH_FORMAT = DOMAIN_GROUP_ROOT
      + "/%s";

  public static String getDomainGroupPath(String domainGroupName) {
    return String.format(DOMAIN_GROUP_PATH_FORMAT, domainGroupName);
  }

  public static void checkExists(ZooKeeper zk, String path) throws DataNotFoundException {
    checkExists(zk, path, "Node does not exist at " + path);
  }

  public static void checkExists(ZooKeeper zk, String path, String message) throws DataNotFoundException {
    try {
      if (zk.exists(path, null) == null) {
        throw new DataNotFoundException(message);
      }
    } catch (KeeperException e) {
      // bliu: I have no idea what exception can be thrown (not documented), so
      // I'll just die so that we can find out!
      throw new RuntimeException("The code does not expect an exception here.",
          e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void checkExistsOrDie(ZooKeeper zk, String path)
      throws InterruptedException {
    checkExistsOrDie(zk, path, "Fatal: Node does not exist at " + path);
  }

  public static void checkExistsOrDie(ZooKeeper zk, String path, String message)
      throws InterruptedException {
    try {
      if (zk.exists(path, null) == null) {
        LOG.fatal(message);
        throw new RuntimeException(message);
      }
    } catch (KeeperException e) {
      // bliu: I have no idea what exception can be thrown (not documented), so
      // I'll just die so that we can find out!
      throw new RuntimeException("The code does not expect an exception here.",
          e);
    }
  }

  public static List<String> getChildrenOrDie(ZooKeeper zk, String path)
      throws InterruptedException {
    try {
      return zk.getChildren(path, null);
    } catch (KeeperException e) {
      // The only time we should get this exception is when the node does not
      // exist
      String dieMessage = "Node does not exist at " + path;
      LOG.fatal(dieMessage, e);
      throw new RuntimeException(dieMessage, e);
    }
  }

  public static String getStringOrDie(ZooKeeper zk, String path) {
    try {
      return Bytes.bytesToString(zk.getData(path, null, null));
    } catch (KeeperException e) {
      // The only time a KeeperException is thrown from ZooKeeper#getData is
      // when the node doesn't exist.
      String dieMessage = "Node does not exist at " + path;
      LOG.fatal(dieMessage, e);
      throw new RuntimeException(dieMessage, e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static int getIntOrDie(ZooKeeper zk, String path)
      throws InterruptedException {
    try {
      return Bytes.bytesToInt(zk.getData(path, null, null));
    } catch (KeeperException e) {
      // The only time a KeeperException is thrown from ZooKeeper#getData is
      // when the node doesn't exist.
      String dieMessage = "Node does not exist at " + path;
      LOG.fatal(dieMessage, e);
      throw new RuntimeException(dieMessage, e);
    }
  }

  public static Stat setDataOrDie(ZooKeeper zk, String path, byte[] data)
      throws InterruptedException {
    try {
      return zk.setData(path, data, -1);
    } catch (KeeperException e) {
      // The only time we should get a KeeperException is if the node doesn't
      // exist
      String message = "Node does not exist at " + path;
      LOG.fatal(message, e);
      throw new RuntimeException(message, e);
    }
  }

  public static void createNodeOrFailSilently(ZooKeeper zk, String path)
      throws InterruptedException {
    setDataOrFailSilently(zk, path, null);
  }

  public static void setDataOrFailSilently(ZooKeeper zk, String path,
      byte[] data) throws InterruptedException {
    try {
      if (zk.exists(path, null) != null) {
        zk.setData(path, data, -1);
      } else {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      LOG.info(e);
    }
  }

  public static void createNodeRecursively(ZooKeeper zk, String path)
      throws InterruptedException {
    try {
      zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException.NoNodeException e) {
      String parentPath = path.substring(0, path.lastIndexOf('/'));
      createNodeRecursively(zk, parentPath);
      createNodeRecursively(zk, path); // Potential for infinite loop, but
                                       // unlikely
    } catch (KeeperException e) {
      LOG.warn(e);
    }
  }

  public static void rmrf(ZooKeeper zk, String path)
      throws InterruptedException, KeeperException {
    deleteNodeRecursively(zk, path);
  }

  public static void deleteNodeRecursively(ZooKeeper zk, String path)
      throws InterruptedException, KeeperException {
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
