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
package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.coordinator.AbstractHostDomainPartition;
import com.rapleaf.hank.zookeeper.WatchedInt;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;

public class ZkHostDomainPartition extends AbstractHostDomainPartition {
  private static final String CURRENT_VERSION_PATH_SEGMENT = "/current_version";
  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "/updating_to_version";
  private static final String DELETABLE_PATH_SEGMENT = "/selected_for_deletion";
  private final String path;
  private final int partNum;
  private final ZooKeeperPlus zk;
  private final String countersPath;

  private final WatchedInt currentDomainGroupVersion;
  private final WatchedInt updatingToDomainGroupVersion;

  public ZkHostDomainPartition(ZooKeeperPlus zk, String path)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    this.countersPath = path + "/counters";
    String[] toks = path.split("/");
    this.partNum = Integer.parseInt(toks[toks.length - 1]);

    // TODO: remove post-migration
    if (zk.exists(path + CURRENT_VERSION_PATH_SEGMENT, false) == null) {
      zk.create(path + CURRENT_VERSION_PATH_SEGMENT, null, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    }

    if (zk.exists(path + UPDATING_TO_VERSION_PATH_SEGMENT, false) == null) {
      zk.create(path + UPDATING_TO_VERSION_PATH_SEGMENT, null, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    }

    currentDomainGroupVersion = new WatchedInt(zk, path + CURRENT_VERSION_PATH_SEGMENT);
    updatingToDomainGroupVersion = new WatchedInt(zk, path + UPDATING_TO_VERSION_PATH_SEGMENT);
  }

  @Override
  public Integer getCurrentDomainGroupVersion() throws IOException {
    return currentDomainGroupVersion.get();
  }

  @Override
  public int getPartNum() {
    return partNum;
  }

  @Override
  public Integer getUpdatingToDomainGroupVersion() throws IOException {
    return updatingToDomainGroupVersion.get();
  }

  @Override
  public boolean isDeletable() throws IOException {
    try {
      return (zk.exists(path + DELETABLE_PATH_SEGMENT, false) != null);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setCurrentDomainGroupVersion(int version) throws IOException {
    try {
      currentDomainGroupVersion.set(version);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setUpdatingToDomainGroupVersion(Integer version) throws IOException {
    try {
      updatingToDomainGroupVersion.set(version);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setDeletable(boolean deletable) throws IOException {
    try {
      String p = path + DELETABLE_PATH_SEGMENT;
      if (deletable)
        zk.setOrCreate(p, 0, CreateMode.PERSISTENT);
      else
        zk.deleteIfExists(p);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static ZkHostDomainPartition create(ZooKeeperPlus zk, String domainPath, int partNum, int initialDomainGroupVersion) throws IOException {
    try {
      String hdpPath = domainPath + "/" + partNum;
      zk.create(hdpPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zk.create(hdpPath + CURRENT_VERSION_PATH_SEGMENT, null, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
      zk.create(hdpPath + UPDATING_TO_VERSION_PATH_SEGMENT, initialDomainGroupVersion,
        CreateMode.PERSISTENT);
      zk.create(hdpPath + "/counters", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      return new ZkHostDomainPartition(zk, hdpPath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeCount(String countID) throws IOException {
    try {
      String p = countersPath + "/" + countID;
      zk.delete(p, -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setCount(String countID, long count) throws IOException {
    try {
      String p = countersPath + "/" + countID;
      zk.setOrCreate(p, count, CreateMode.PERSISTENT);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Long getCount(String countID) throws IOException {
    Long data = null;
    try {
      String p = countersPath + "/" + countID;
      if (zk.exists(p, false) != null) {
        data = Long.parseLong(new String(zk.getData(p, false, null)));
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    return data;
  }

  @Override
  public Set<String> getCountKeys() throws IOException {
    try {
      return new HashSet<String>(zk.getChildren(countersPath, false));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete() throws IOException {
    try {
      zk.deleteIfExists(path);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
