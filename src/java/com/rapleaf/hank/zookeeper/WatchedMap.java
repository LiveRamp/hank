package com.rapleaf.hank.zookeeper;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class WatchedMap<T> extends AbstractMap<String, T> {
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
          synchronized (changeMutex) {
            syncMap(internalMap);
          }
      }
    }
  };

  public interface ElementLoader<T> {
    public T load(ZooKeeperPlus zk, String basePath, String relPath);
  }

  private final ZooKeeperPlus zk;
  private final String path;

  private Map<String, T> internalMap;
  private final Object changeMutex = new Object();
  private final ElementLoader<T> elementLoader;

  public WatchedMap(ZooKeeperPlus zk, String basePath, ElementLoader<T> elementLoader) {
    this.zk = zk;
    this.path = basePath;
    this.elementLoader = elementLoader;
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
    synchronized (changeMutex) {
      // if the map is non-null, then it's already loaded and the watching
      // mechanism will take care of everything...
      if (internalMap == null) {
        // ...but if it's not loaded, we need to do the initial population.
        Map<String, T> m = new HashMap<String, T>();
        syncMap(m);
        internalMap = m;
      }
    }
  }

  private void syncMap(Map<String, T> m) {
    try {
      final List<String> childrenRelPaths = zk.getChildren(path, watcher);
      for (String relpath : childrenRelPaths) {
        if (!m.containsKey(relpath)) {
          m.put(relpath, elementLoader.load(zk, path, relpath));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception trying to reload contents of " + path, e);
    }
  }
}
