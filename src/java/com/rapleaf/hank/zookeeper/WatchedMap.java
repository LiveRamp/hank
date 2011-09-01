package com.rapleaf.hank.zookeeper;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class WatchedMap<T> extends AbstractMap<String, T> {
  public interface CompletionAwaiter {
    public void completed(String relPath);
  }

  public interface CompletionDetector {
    public void detectCompletion(ZooKeeperPlus zk, String basePath, String relPath, CompletionAwaiter awaiter) throws KeeperException, InterruptedException;
  }

  private static final CompletionDetector ALWAYS_COMPLETE = new CompletionDetector() {
    @Override
    public void detectCompletion(ZooKeeperPlus zk, String basePath, String relPath, CompletionAwaiter awaiter) throws KeeperException, InterruptedException {
      awaiter.completed(relPath);
    }
  };

  // NOTE: I have no idea why it's necessary to have an internal Watcher impl
  // instead of just directly implementing Watcher, but this works and the other
  // way doesn't.
  private final Watcher watcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      // only operate if we're connected
      if (event.getState() != KeeperState.SyncConnected) {
        return;
      }
      switch (event.getType()) {
        case NodeChildrenChanged:
          synchronized (notifyMutex) {
            syncMap();
          }
      }
    }
  };

  public interface ElementLoader<T> {
    public T load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException;
  }

  private final ZooKeeperPlus zk;
  private final String path;

  private final Map<String, T> internalMap = new HashMap<String, T>();
  private final Object notifyMutex = new Object();
  private final ElementLoader<T> elementLoader;
  private final CompletionDetector completionDetector;

  private CompletionAwaiter awaiter = new CompletionAwaiter() {
    @Override
    public void completed(String relPath) {
      synchronized (internalMap) {
        try {
          final T element = elementLoader.load(zk, path, relPath);
          if (element == null) {
            return;
          }
          internalMap.put(relPath, element);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  };

  private boolean loaded;

  public WatchedMap(ZooKeeperPlus zk, String basePath, ElementLoader<T> elementLoader) {
    this(zk, basePath, elementLoader, ALWAYS_COMPLETE);
  }

  public WatchedMap(ZooKeeperPlus zk, String basePath, ElementLoader<T> elementLoader, CompletionDetector completionDetector) {
    this.zk = zk;
    this.path = basePath;
    this.elementLoader = elementLoader;
    this.completionDetector = completionDetector;
  }

  public ZooKeeperPlus getZk() {
    return zk;
  }

  public String getCollectionPath() {
    return path;
  }

  public Collection<T> values() {
    ensureLoaded();
    return internalMap.values();
  }

  public Set<Map.Entry<String, T>> entrySet() {
    ensureLoaded();
    return internalMap.entrySet();
  }

  public Set<String> keySet() {
    ensureLoaded();
    return internalMap.keySet();
  }

  private void ensureLoaded() {
    // this lock is important so that when changes start happening, we
    // won't run into any concurrency issues
    synchronized (notifyMutex) {
      // if the map is non-null, then it's already loaded and the watching
      // mechanism will take care of everything...
      if (!loaded) {
        // ...but if it's not loaded, we need to do the initial population.
        syncMap();
        loaded = true;
      }
    }
  }

  private void syncMap() {
    try {
      final List<String> childrenRelPaths = zk.getChildren(path, watcher);
      for (String relpath : childrenRelPaths) {
        if (!internalMap.containsKey(relpath)) {
          completionDetector.detectCompletion(zk, path, relpath, awaiter);
        }
      }
      Set<String> deletedKeys = new HashSet<String>(internalMap.keySet());
      deletedKeys.removeAll(childrenRelPaths);
      for (String deletedKey : deletedKeys) {
        internalMap.remove(deletedKey);
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception trying to reload contents of " + path, e);
    }
  }

  @Override
  public T put(String arg0, T arg1) {
    return internalMap.put(arg0, arg1);
  }
}
