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
import com.rapleaf.hank.zookeeper.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class ZkHostDomainPartition extends AbstractHostDomainPartition {
  private static final String CURRENT_VERSION_PATH_SEGMENT = "current_version";
  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "updating_to_version";
  private static final String DELETABLE_PATH_SEGMENT = "selected_for_deletion";
  private static final String STATISTICS_PATH_SEGMENT = "statistics";
  private final String path;
  private final int partNum;
  private final ZooKeeperPlus zk;

  private final WatchedInt currentDomainGroupVersion;
  private final WatchedInt updatingToDomainGroupVersion;
  private final WatchedBoolean deletable;
  private final WatchedMap<String> statistics;

  public static ZkHostDomainPartition create(ZooKeeperPlus zk, String domainPath, int partNum, int initialDomainGroupVersion) throws IOException {
    try {
      String hdpPath = ZkPath.append(domainPath, Integer.toString(partNum));
      zk.create(hdpPath, null);
      zk.create(ZkPath.append(hdpPath, CURRENT_VERSION_PATH_SEGMENT), null);
      zk.createInt(ZkPath.append(hdpPath, UPDATING_TO_VERSION_PATH_SEGMENT), initialDomainGroupVersion);
      zk.create(ZkPath.append(hdpPath, DELETABLE_PATH_SEGMENT), Boolean.FALSE.toString().getBytes());
      zk.create(ZkPath.append(hdpPath, STATISTICS_PATH_SEGMENT), null);
      zk.create(ZkPath.append(hdpPath, DotComplete.NODE_NAME), null);

      return new ZkHostDomainPartition(zk, hdpPath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public ZkHostDomainPartition(ZooKeeperPlus zk, String path)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    this.partNum = Integer.parseInt(ZkPath.getFilename(path));

    currentDomainGroupVersion = new WatchedInt(zk, ZkPath.append(path, CURRENT_VERSION_PATH_SEGMENT), true);
    updatingToDomainGroupVersion = new WatchedInt(zk, ZkPath.append(path, UPDATING_TO_VERSION_PATH_SEGMENT), true);
    deletable = new WatchedBoolean(zk, ZkPath.append(path, DELETABLE_PATH_SEGMENT), true);
    // TODO: temporary migration fix
    try {
      zk.create(ZkPath.append(path, STATISTICS_PATH_SEGMENT), null);
    } catch (Exception e) {
      // swallow
    }
    statistics = new WatchedMap<String>(zk, ZkPath.append(path, STATISTICS_PATH_SEGMENT),
        new WatchedMap.ElementLoader<String>() {
      @Override
      public String load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        if (!ZkPath.isHidden(relPath)) {
          return zk.getString(ZkPath.append(basePath, relPath));
        } else {
          return null;
        }
      }
    });
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

  @Override
  public void setEphemeralStatistic(String key, String value) throws IOException {
    String path = ZkPath.append(this.path, STATISTICS_PATH_SEGMENT, key);
    try {
      zk.setOrCreate(path, value, CreateMode.EPHEMERAL);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public String getStatistic(String key) throws IOException {
    return statistics.get(key);
  }

  @Override
  public void deleteStatistic(String key) throws IOException {
    statistics.remove(key);
    String path = ZkPath.append(this.path, STATISTICS_PATH_SEGMENT, key);
    try {
      zk.deleteIfExists(path);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
