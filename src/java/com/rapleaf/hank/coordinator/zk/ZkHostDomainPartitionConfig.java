package com.rapleaf.hank.coordinator.zk;

import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;

public class ZkHostDomainPartitionConfig implements HostDomainPartitionConfig {

  public ZkHostDomainPartitionConfig(ZooKeeper zk, String path) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public int getCurrentDomainGroupVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getPartNum() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getUpdatingToDomainGroupVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setCurrentDomainGroupVersion() {
    // TODO Auto-generated method stub

  }

  @Override
  public void setUpdatingToDomainGroupVersion() {
    // TODO Auto-generated method stub

  }

}
