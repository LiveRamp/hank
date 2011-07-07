package com.rapleaf.hank.coordinator.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import com.rapleaf.hank.zookeeper.WatchedMap.CompletionAwaiter;
import com.rapleaf.hank.zookeeper.WatchedMap.CompletionDetector;

public class DotComplete implements CompletionDetector {
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
    if (zk.exists(basePath + "/" + relPath + "/.complete", new CreationWatcher(relPath, awaiter)) != null) {
      awaiter.completed(relPath);
    }
  }
}
