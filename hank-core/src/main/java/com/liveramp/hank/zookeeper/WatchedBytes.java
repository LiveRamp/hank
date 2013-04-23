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

import org.apache.zookeeper.KeeperException;

public class WatchedBytes extends WatchedNode<byte[]> {

  /**
   * Start watching a node, optionnaly waiting for it to be created
   *
   * @param zk
   * @param nodePath
   * @param waitForCreation
   * @throws org.apache.zookeeper.KeeperException
   *
   * @throws InterruptedException
   */
  public WatchedBytes(final ZooKeeperPlus zk,
                      final String nodePath,
                      boolean waitForCreation) throws KeeperException, InterruptedException {
    super(zk, nodePath, waitForCreation);
  }

  @Override
  protected byte[] decode(byte[] data) {
    return data;
  }

  @Override
  protected byte[] encode(byte[] v) {
    return v;
  }
}
