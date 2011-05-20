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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.coordinator.AbstractHostDomainPartition;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
public class ZkHostDomainPartition extends AbstractHostDomainPartition {
  private static final String CURRENT_VERSION_PATH_SEGMENT = "/current_version";
  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "/updating_to_version";
  private final String path;
  private final int partNum;
  private final ZooKeeperPlus zk;

  public ZkHostDomainPartition(ZooKeeperPlus zk, String path) {
    this.zk = zk;
    this.path = path;
    String[] toks = path.split("/");
    this.partNum = Integer.parseInt(toks[toks.length - 1]);
  }

  @Override
  public Integer getCurrentDomainGroupVersion() throws IOException {
    try {
      if (zk.exists(path + CURRENT_VERSION_PATH_SEGMENT, false) != null) {
        return zk.getIntOrNull(path + CURRENT_VERSION_PATH_SEGMENT);
      }
      return null;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int getPartNum() {
    return partNum;
  }

  @Override
  public Integer getUpdatingToDomainGroupVersion() throws IOException {
    try {
      if (zk.exists(path + UPDATING_TO_VERSION_PATH_SEGMENT, false) != null) {
        return zk.getIntOrNull(path + UPDATING_TO_VERSION_PATH_SEGMENT);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    return null;
  }

  @Override
  public void setCurrentDomainGroupVersion(int version) throws IOException {
    try {
      String p = path + CURRENT_VERSION_PATH_SEGMENT;
      zk.setOrCreate(p, version, CreateMode.PERSISTENT);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setUpdatingToDomainGroupVersion(Integer version) throws IOException {
    try {
      String p = path + UPDATING_TO_VERSION_PATH_SEGMENT;
      if (version == null) {
        zk.delete(p, -1);
        return;
      }
      zk.setOrCreate(p, version, CreateMode.PERSISTENT);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static ZkHostDomainPartition create(ZooKeeperPlus zk, String domainPath, int partNum, int initialDomainGroupVersion) throws IOException {
    try {
      String hdpPath = domainPath + "/" + partNum;
      zk.create(hdpPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zk.create(hdpPath + UPDATING_TO_VERSION_PATH_SEGMENT, ("" + initialDomainGroupVersion).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      return new ZkHostDomainPartition(zk, hdpPath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
