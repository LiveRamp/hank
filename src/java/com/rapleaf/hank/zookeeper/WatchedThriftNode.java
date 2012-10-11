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

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.zookeeper.KeeperException;

public class WatchedThriftNode<T extends TBase> extends WatchedNode<T> {

  protected TDeserializer deserializer;
  protected TSerializer serializer;

  public WatchedThriftNode(final ZooKeeperPlus zk,
                           final String nodePath,
                           boolean waitForCreation,
                           boolean create,
                           T emptyValue) throws KeeperException, InterruptedException {
    super(zk, nodePath, waitForCreation, create, emptyValue);
  }

  @Override
  protected T decode(byte[] data) {
    if (data == null) {
      return null;
    } else {
      if (deserializer == null) {
        deserializer = new TDeserializer(new TCompactProtocol.Factory());
      }
      T result = (T) initialValue.deepCopy();
      try {
        deserializer.deserialize(result, data);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
      return result;
    }
  }

  @Override
  protected byte[] encode(T v) {
    if (v == null) {
      return null;
    } else {
      if (serializer == null) {
        serializer = new TSerializer(new TCompactProtocol.Factory());
      }
      try {
        return serializer.serialize(v);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
