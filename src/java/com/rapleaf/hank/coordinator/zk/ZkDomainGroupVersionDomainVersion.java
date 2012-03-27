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

import com.rapleaf.hank.coordinator.AbstractDomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class ZkDomainGroupVersionDomainVersion extends AbstractDomainGroupVersionDomainVersion {
  private final Domain domain;
  private final Integer version;
  private final ZooKeeperPlus zk;
  private final String path;

  public ZkDomainGroupVersionDomainVersion(ZooKeeperPlus zk, String path, Domain domain) throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    this.domain = domain;
    version = Integer.valueOf(zk.getString(path));
  }

  @Override
  public Domain getDomain() {
    return domain;
  }

  @Override
  public Integer getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "ZkDomainGroupVersionDomainVersion [domain=" + domain
        + ", versionNumber=" + version + "]";
  }

  @Override
  public void delete() throws IOException {
    try {
      zk.deleteNodeRecursively(path);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
