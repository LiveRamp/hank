/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.zookeeper;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class WatchedMap<T> extends AbstractMap<String, T> {

  private static final Logger LOG = LoggerFactory.getLogger(WatchedMap.class);

  private static final int NUM_CONCURRENT_COMPLETION_DETECTORS = 16;
  private static final int NUM_NEW_CHILDREN_CONCURRENT_DETECT_COMPLETION_THRESHOLD = 256;
  private static final long COMPLETION_DETECTION_EXECUTOR_TERMINATION_CHECK_PERIOD = 5; // In millisecond

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
          boolean keysChanged = false;
          synchronized (notifyMutex) {
            keysChanged = syncMap();
          }
          if (keysChanged) {
            fireListeners();
          }
          break;
      }
    }
  };

  public interface ElementLoader<T> {
    public T load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException, IOException;
  }

  private final ZooKeeperPlus zk;
  private final String path;

  private final Map<String, T> internalMap = new HashMap<String, T>();
  private final Object notifyMutex = new Object();
  private final ElementLoader<T> elementLoader;
  private final CompletionDetector completionDetector;
  private final Set<WatchedMapListener<T>> listeners = new HashSet<WatchedMapListener<T>>();

  private CompletionAwaiter awaiter = new CompletionAwaiter() {
    @Override
    public void completed(String relPath) {
      try {
        final T element = elementLoader.load(zk, path, relPath);
        if (element == null) {
          return;
        }
        synchronized (internalMap) {
          internalMap.put(relPath, element);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
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

    if (elementLoader == null) {
      throw new RuntimeException("WatchedMap cannot be configured with a null element loader.");
    }
    if (completionDetector == null) {
      throw new RuntimeException("WatchedMap cannot be configured with a null completion detector.");
    }
  }

  public boolean isLoaded() {
    return loaded;
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
    boolean keysChanged = false;
    synchronized (notifyMutex) {
      // if the map is non-null, then it's already loaded and the watching
      // mechanism will take care of everything...
      if (!loaded) {
        // ...but if it's not loaded, we need to do the initial population.
        keysChanged = syncMap();
        loaded = true;
      }
    }
    if (keysChanged) {
      fireListeners();
    }
  }

  // Return true iff the list of keys has changed
  private boolean syncMap() {
    try {
      final List<String> childrenRelPaths = zk.getChildren(path, watcher);
      // Detect new children
      List<String> newChildrenRelPaths = null;
      for (String relpath : childrenRelPaths) {
        if (!internalMap.containsKey(relpath)) {
          if (newChildrenRelPaths == null) {
            newChildrenRelPaths = new ArrayList<String>();
          }
          newChildrenRelPaths.add(relpath);
        }
      }

      // Load new children
      if (newChildrenRelPaths != null) {
        // If number of new children is below a threshold, load them sequentially, otherwise do it concurrently
        if (newChildrenRelPaths.size() < NUM_NEW_CHILDREN_CONCURRENT_DETECT_COMPLETION_THRESHOLD) {
          for (String relPath : newChildrenRelPaths) {
            completionDetector.detectCompletion(zk, path, relPath, awaiter);
          }
        } else {
          detectCompletionConcurrently(zk, path, newChildrenRelPaths, awaiter, completionDetector);
        }
      }

      Set<String> deletedKeys = new HashSet<String>(internalMap.keySet());
      deletedKeys.removeAll(childrenRelPaths);
      for (String deletedKey : deletedKeys) {
        internalMap.remove(deletedKey);
      }

      // Return true iff the list of keys has changed
      return ((newChildrenRelPaths != null && newChildrenRelPaths.size() > 0) || deletedKeys.size() > 0);
    } catch (Exception e) {
      throw new RuntimeException("Exception trying to reload contents of " + path, e);
    }
  }

  private void fireListeners() {
    synchronized (listeners) {
      for (WatchedMapListener<T> listener : listeners) {
        listener.onWatchedMapChange(this);
      }
    }
  }


  private static class DetectCompletionRunnable implements Runnable {

    private final ZooKeeperPlus zk;
    private final String path;
    private final String relPath;
    private final CompletionAwaiter awaiter;
    private final CompletionDetector completionDetector;

    public DetectCompletionRunnable(ZooKeeperPlus zk,
                                    String path,
                                    String relPath,
                                    CompletionAwaiter awaiter,
                                    CompletionDetector completionDetector) {
      this.zk = zk;
      this.path = path;
      this.relPath = relPath;
      this.awaiter = awaiter;
      this.completionDetector = completionDetector;
    }

    @Override
    public void run() {
      try {
        completionDetector.detectCompletion(zk, path, relPath, awaiter);
      } catch (Exception e) {
        LOG.error("Exception while detecting completion for " + path + "/" + relPath, e);
        throw new RuntimeException(e);
      }
    }
  }

  private static synchronized void detectCompletionConcurrently(ZooKeeperPlus zk,
                                                                String path,
                                                                Collection<String> relPaths,
                                                                CompletionAwaiter awaiter,
                                                                CompletionDetector completionDetector) {
    final ExecutorService completionDetectionExecutor =
        Executors.newFixedThreadPool(NUM_CONCURRENT_COMPLETION_DETECTORS,
            new ThreadFactory() {
              private int threadId = 0;

              @Override
              public Thread newThread(Runnable runnable) {
                return new Thread(runnable, "Completion Detector #" + threadId++);
              }
            });
    for (String relPath : relPaths) {
      completionDetectionExecutor.execute(new DetectCompletionRunnable(zk, path, relPath, awaiter, completionDetector));
    }
    completionDetectionExecutor.shutdown();
    boolean terminated = false;
    while (!terminated) {
      try {
        terminated =
            completionDetectionExecutor.awaitTermination(COMPLETION_DETECTION_EXECUTOR_TERMINATION_CHECK_PERIOD,
                TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        completionDetectionExecutor.shutdownNow();
      }
    }
  }

  @Override
  public T put(String key, T value) {
    return internalMap.put(key, value);
  }

  @Override
  public T get(Object key) {
    ensureLoaded();
    return internalMap.get(key);
  }

  public void addListener(WatchedMapListener<T> listener) {
    synchronized (listeners) {
      listeners.add(listener);
    }
  }

  public void removeListener(WatchedMapListener<T> listener) {
    synchronized (listeners) {
      listeners.remove(listener);
    }
  }
}
