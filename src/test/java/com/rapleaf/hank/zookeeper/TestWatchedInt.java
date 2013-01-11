/**
 *  Copyright 2012 Rapleaf
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

package com.rapleaf.hank.zookeeper;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.util.Condition;
import com.rapleaf.hank.util.WaitUntil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class TestWatchedInt extends ZkTestCase {

  public void testIt() throws Exception {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = ZkPath.append(getRoot(), "watchedNode");
    zk.create(nodePath, "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final WatchedInt wi = new WatchedInt(zk, nodePath, true);
    assertEquals(Integer.valueOf(1), wi.get());

    zk.setData(nodePath, "55".getBytes(), -1);
    WaitUntil.condition(new Condition() {
      @Override
      public boolean test() {
        Integer v = wi.get();
        return v != null && v == 55;
      }
    });
    assertEquals(Integer.valueOf(55), wi.get());

    zk.setData(nodePath, null, -1);
    WaitUntil.condition(new Condition() {
      @Override
      public boolean test() {
        return wi.get() == null;
      }
    });
    assertNull(wi.get());

    final WatchedInt wi2 = new WatchedInt(zk, nodePath, true);
    WaitUntil.condition(new Condition() {
      @Override
      public boolean test() {
        return wi2.get() == null;
      }
    });
    assertNull(wi2.get());
    wi2.set(22);
    WaitUntil.condition(new Condition() {
      @Override
      public boolean test() {
        Integer v = wi2.get();
        return v != null && v == 22;
      }
    });
    assertEquals(Integer.valueOf(22), wi2.get());
    assertEquals(Integer.valueOf(22), wi.get());
  }

  public void testUpdate() throws InterruptedException, KeeperException {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = ZkPath.append(getRoot(), "watchedNode");
    zk.create(nodePath, "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final WatchedInt wi = new WatchedInt(zk, nodePath, true);
    assertEquals(Integer.valueOf(0), wi.get());

    WatchedNodeUpdater<Integer> incrementer = new WatchedNodeUpdater<Integer>() {
      @Override
      public Integer update(Integer current) {
        if (current == null) {
          return 0;
        } else {
          return current + 1;
        }
      }
    };
    int finalValue = 64;
    for (int i = 0; i < finalValue; ++i) {
      wi.update(incrementer);
    }
    Thread.sleep(100);
    assertEquals(Integer.valueOf(finalValue), wi.get());
  }

  public void testWaitForCreation() throws InterruptedException, KeeperException {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = ZkPath.append(getRoot(), "watchedNode");
    WatchedInt wi = new WatchedInt(zk, nodePath, false);

    assertNull(wi.get());

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // Wait before creating it
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        try {
          zk.create(nodePath, "42".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // Try waiting
    t.start();
    wi = new WatchedInt(zk, nodePath, true);
    assertEquals(Integer.valueOf(42), wi.get());
  }

  public void testCreateAndWaitForCreation() throws InterruptedException, KeeperException {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = ZkPath.append(getRoot(), "watchedNode");
    WatchedInt wi = new WatchedInt(zk, nodePath, true, CreateMode.PERSISTENT, 84, 0);
    assertEquals(Integer.valueOf(84), wi.get());
  }
}
