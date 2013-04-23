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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class WatchedInt extends WatchedNode<Integer> {

  public WatchedInt(ZooKeeperPlus zk, String nodePath, boolean waitForCreation) throws KeeperException, InterruptedException {
    super(zk, nodePath, waitForCreation);
  }

  public WatchedInt(ZooKeeperPlus zk,
                    String nodePath,
                    boolean waitForCreation,
                    CreateMode createMode,
                    Integer initialValue,
                    Integer emptyValue) throws KeeperException, InterruptedException {
    super(zk, nodePath, waitForCreation, createMode, initialValue, emptyValue);
  }

  public static Integer get(ZooKeeperPlus zk, String nodePath) throws InterruptedException, KeeperException {
    return decodeValue(zk.getData(nodePath, null, null));
  }

  protected static Integer decodeValue(byte[] data) {
    if (data == null) {
      return null;
    }
    return Integer.parseInt(new String(data));
  }

  protected static byte[] encodeValue(Integer v) {
    if (v == null) {
      return null;
    }
    return v.toString().getBytes();
  }

  @Override
  protected Integer decode(byte[] data) {
    return decodeValue(data);
  }

  @Override
  protected byte[] encode(Integer v) {
    return encodeValue(v);
  }
}
