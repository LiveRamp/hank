package com.liveramp.hank.zookeeper;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import com.liveramp.hank.zookeeper.WatchedMap.CompletionAwaiter;
import com.liveramp.hank.zookeeper.WatchedMap.CompletionDetector;
import com.liveramp.hank.zookeeper.WatchedMap.ElementLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestWatchedMap extends ZkTestCase {
  private static final class StringElementLoader implements ElementLoader<String> {
    @Override
    public String load(ZooKeeperPlus zk, String basePath, String relPath) {
      try {
        return new String(zk.getData(ZkPath.append(basePath, relPath), false, new Stat()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testIt() throws Exception {

    final ZooKeeperPlus zk = getZk();
    final String colRoot = ZkPath.append(getRoot(), "collection");
    zk.create(colRoot, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final ElementLoader<String> elementLoader = new ElementLoader<String>() {
      @Override
      public String load(ZooKeeperPlus zk, String basePath, String relPath) {
        try {
          return new String(zk.getData(ZkPath.append(basePath, relPath), false, new Stat()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    final WatchedMap<String> c1 = new WatchedMap<String>(zk, colRoot, elementLoader);
    dumpZk();

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return 0 == c1.size();
      }
    });
    assertEquals(0, c1.size());
    zk.create(ZkPath.append(colRoot, "first"), "data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return 1 == c1.size();
      }
    });
    assertEquals(1, c1.size());
  }

  @Test
  public void testCompletionDetector() throws Exception {
    final ElementLoader<String> elementLoader = new StringElementLoader();
    final AtomicBoolean b = new AtomicBoolean(false);
    CompletionDetector completionDetector = new CompletionDetector() {
      @Override
      public void detectCompletion(ZooKeeperPlus zk, String basePath, final String relPath, final CompletionAwaiter awaiter) throws KeeperException, InterruptedException {
        b.set(true);
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              Thread.sleep(1500);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            awaiter.completed(relPath);
          }
        }).start();
      }
    };
    final WatchedMap<String> m = new WatchedMap<String>(getZk(), getRoot(), elementLoader,
        completionDetector);
    // no elements yet, so should be empty
    assertEquals(0, m.size());
    // create an element
    getZk().create(ZkPath.append(getRoot(), "node"), "blah".getBytes(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    // wait for notification to propagate
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return b.get();
      }
    });
    // the detector should have been invoked...
    assertTrue(b.get());
    // ...but it still shouldn't have come through to the actual map, since
    // there's a delay in the completion detector.
    assertEquals(0, m.size());
    // after waiting a bit, the completion detector should notify the awaiter
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return 1 == m.size();
      }
    });
    assertEquals(1, m.size());
  }

  @Test
  public void testDeletion() throws Exception {
    getZk().create(ZkPath.append(getRoot(), "map"), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    getZk().create(ZkPath.append(getRoot(), "map/1"), "2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final WatchedMap<String> m = new WatchedMap<String>(getZk(), ZkPath.append(getRoot(), "map"), new StringElementLoader());
    assertEquals(new HashMap<String, String>() {{
      put("1", "2");
    }}, m);
    getZk().delete(ZkPath.append(getRoot(), "map/1"), 0);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return Collections.EMPTY_MAP.equals(m);
      }
    });
    assertEquals(Collections.EMPTY_MAP, m);
  }
}
