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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

public class TestWatchedCompactMap extends ZkTestCase {

  final String nodePath = ZkPath.append(getRoot(), "watchedNode");

  private class Value {

    private final int value;

    public Value(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  private class ValueEncoder implements WatchedCompactMap.ValueEncoder<Value> {

    @Override
    public String encode(Value value) {
      if (value == null) {
        return "";
      } else {
        return "v-" + value.getValue();
      }
    }

    @Override
    public Value decode(String value) {
      if (value.length() == 0) {
        return null;
      } else {
        return new Value(Integer.valueOf(value.split("-")[1]));
      }
    }
  }

  public void testIt() throws Exception {
    getZk().create(nodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    WatchedCompactMap<Value> map = new WatchedCompactMap<Value>(getZk(), nodePath, true, new ValueEncoder());
    WatchedCompactMap<Value> sameMap = new WatchedCompactMap<Value>(getZk(), nodePath, true, new ValueEncoder());
    assertEquals(null, map.get("k1"));
    assertEquals(null, map.put("k1", new Value(1)));
    assertEquals(null, map.put("knull", null));

    Thread.sleep(100);

    assertEquals(1, map.get("k1").getValue());
    assertEquals(null, map.get("knull"));

    assertEquals(1, sameMap.get("k1").getValue());
    assertEquals(null, sameMap.get("knull"));

    sameMap.put("k1", new Value(2));

    Thread.sleep(100);

    assertEquals(2, map.get("k1").getValue());
    assertEquals(2, sameMap.get("k1").getValue());

    map.remove("k1");

    Thread.sleep(100);

    assertFalse(map.containsKey("k1"));
    assertEquals(null, map.get("k1"));

    assertFalse(sameMap.containsKey("k1"));
    assertEquals(null, sameMap.get("k1"));
  }
}
