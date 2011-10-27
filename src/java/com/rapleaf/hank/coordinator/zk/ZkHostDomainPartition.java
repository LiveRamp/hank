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

import com.rapleaf.hank.coordinator.AbstractHostDomainPartition;
import com.rapleaf.hank.zookeeper.WatchedBoolean;
import com.rapleaf.hank.zookeeper.WatchedInt;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ZkHostDomainPartition extends AbstractHostDomainPartition {
  private static final String CURRENT_VERSION_PATH_SEGMENT = "current_version";
  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "updating_to_version";
  private static final String DELETABLE_PATH_SEGMENT = "selected_for_deletion";
  private static final String COUNTERS_PATH_SEGMENT = "counters";
  private final String path;
  private final int partNum;
  private final ZooKeeperPlus zk;
  private final String countersPath;

  private final WatchedInt currentDomainGroupVersion;
  private final WatchedInt updatingToDomainGroupVersion;
  private final WatchedBoolean deletable;

  public static ZkHostDomainPartition create(ZooKeeperPlus zk, String domainPath, int partNum, int initialDomainGroupVersion) throws IOException {
    try {
      String hdpPath = ZkPath.append(domainPath, Integer.toString(partNum));
      zk.create(hdpPath, null);
      zk.create(ZkPath.append(hdpPath, CURRENT_VERSION_PATH_SEGMENT), null);
      zk.createInt(ZkPath.append(hdpPath, UPDATING_TO_VERSION_PATH_SEGMENT), initialDomainGroupVersion);
      zk.create(ZkPath.append(hdpPath, DELETABLE_PATH_SEGMENT), Boolean.FALSE.toString().getBytes());
      try {
        zk.create(ZkPath.append(hdpPath, COUNTERS_PATH_SEGMENT), null);
      } catch (KeeperException.NodeExistsException e) {
        // ignore
      }
      try {
        zk.create(ZkPath.append(hdpPath, DotComplete.NODE_NAME), null);
      } catch (KeeperException.NodeExistsException e) {
        // ignore
      }

      return new ZkHostDomainPartition(zk, hdpPath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public ZkHostDomainPartition(ZooKeeperPlus zk, String path)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    this.countersPath = ZkPath.append(path, "counters");
    this.partNum = Integer.parseInt(ZkPath.getFilename(path));

    currentDomainGroupVersion = new WatchedInt(zk, ZkPath.append(path, CURRENT_VERSION_PATH_SEGMENT), true, null);
    updatingToDomainGroupVersion = new WatchedInt(zk, ZkPath.append(path, UPDATING_TO_VERSION_PATH_SEGMENT),
        true, null);
    deletable = new WatchedBoolean(zk, ZkPath.append(path, DELETABLE_PATH_SEGMENT), true, false);
  }

  @Override
  public Integer getCurrentDomainGroupVersion() throws IOException {
    return currentDomainGroupVersion.get();
  }

  @Override
  public int getPartitionNumber() {
    return partNum;
  }

  @Override
  public Integer getUpdatingToDomainGroupVersion() throws IOException {
    return updatingToDomainGroupVersion.get();
  }

  @Override
  public boolean isDeletable() throws IOException {
    return deletable.get();
  }

  @Override
  public void setDeletable(boolean deletable) throws IOException {
    try {
      this.deletable.set(deletable);
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
  public void removeCount(String countID) throws IOException {
    try {
      String p = ZkPath.append(countersPath, countID);
      zk.delete(p, -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setCount(String countID, long count) throws IOException {
    try {
      String p = ZkPath.append(countersPath, countID);
      zk.setOrCreate(p, count, CreateMode.PERSISTENT);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Long getCount(String countID) throws IOException {
    Long data = null;
    try {
      String p = ZkPath.append(countersPath, countID);
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
      zk.deleteNodeRecursively(path);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public String getPath() {
    return path;
  }
}
