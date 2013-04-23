package com.liveramp.hank.coordinator.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.liveramp.hank.zookeeper.ZkPath;
import com.liveramp.hank.zookeeper.ZooKeeperPlus;
import com.liveramp.hank.zookeeper.WatchedMap.CompletionAwaiter;
import com.liveramp.hank.zookeeper.WatchedMap.CompletionDetector;

public class DotComplete implements CompletionDetector {

  public static final String NODE_NAME = ".complete";

  public static final class CreationWatcher implements Watcher {
    private final String relPath;
    private final CompletionAwaiter awaiter;

    public CreationWatcher(String relPath, CompletionAwaiter awaiter) {
      this.relPath = relPath;
      this.awaiter = awaiter;
    }

    @Override
    public void process(WatchedEvent event) {
      if (event.getState() != KeeperState.SyncConnected) {
        return;
      }
      switch (event.getType()) {
        case NodeCreated:
          awaiter.completed(relPath);
      }
    }
  }

  @Override
  public void detectCompletion(ZooKeeperPlus zk, String basePath, String relPath, CompletionAwaiter awaiter) throws KeeperException, InterruptedException {
    if (zk.exists(ZkPath.append(basePath, relPath, NODE_NAME), new CreationWatcher(relPath, awaiter)) != null) {
      awaiter.completed(relPath);
    }
  }
}
