package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
public class ZkHostDomainPartitionConfig implements HostDomainPartitionConfig {

  private static final String CURRENT_VERSION_PATH_SEGMENT = "/current_version";
  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "/updating_to_version";
  private final ZooKeeper zk;
  private final String path;
  private final int partNum;

  public ZkHostDomainPartitionConfig(ZooKeeper zk, String path) {
    this.zk = zk;
    this.path = path;
    String[] toks = path.split("/");
    this.partNum = Integer.parseInt(toks[toks.length - 1]);
  }

  @Override
  public Integer getCurrentDomainGroupVersion() throws IOException {
    try {
      if (zk.exists(path + CURRENT_VERSION_PATH_SEGMENT, false) != null) {
        return Integer.parseInt(new String(zk.getData(path + CURRENT_VERSION_PATH_SEGMENT, false, new Stat())));
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    return null;
  }

  @Override
  public int getPartNum() {
    return partNum;
  }

  @Override
  public Integer getUpdatingToDomainGroupVersion() throws IOException {
    try {
      if (zk.exists(path + UPDATING_TO_VERSION_PATH_SEGMENT, false) != null) {
        return Integer.parseInt(new String(zk.getData(path + UPDATING_TO_VERSION_PATH_SEGMENT, false, new Stat())));
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
      if (zk.exists(p, false) == null) {
        zk.create(p, ("" + version).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        zk.setData(p, ("" + version).getBytes(), -1);
      }
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
      if (zk.exists(p, false) == null) {
        zk.create(p, ("" + version).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        zk.setData(p, ("" + version).getBytes(), -1);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static ZkHostDomainPartitionConfig create(ZooKeeper zk, String domainPath, int partNum, int initialDomainGroupVersion) throws IOException {
    try {
      String hdpPath = domainPath + "/" + partNum;
      zk.create(hdpPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zk.create(hdpPath + UPDATING_TO_VERSION_PATH_SEGMENT, ("" + initialDomainGroupVersion).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      return new ZkHostDomainPartitionConfig(zk, hdpPath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
