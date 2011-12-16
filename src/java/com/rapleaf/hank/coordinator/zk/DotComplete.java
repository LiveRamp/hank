package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.zookeeper.NodeCreationBarrier;
import com.rapleaf.hank.zookeeper.WatchedMap.CompletionAwaiter;
import com.rapleaf.hank.zookeeper.WatchedMap.CompletionDetector;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.KeeperException;

public class DotComplete implements CompletionDetector {

  public static final String NODE_NAME = ".complete";

  @Override
  public void detectCompletion(ZooKeeperPlus zk, String basePath, String relPath, CompletionAwaiter awaiter)
      throws KeeperException, InterruptedException {
    NodeCreationBarrier.block(zk, ZkPath.append(basePath, relPath, NODE_NAME));
    awaiter.completed(relPath);
  }
}
