/**
 *  Copyright 2012 LiveRamp
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

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class WatchedThriftNode<T extends TBase> extends WatchedNode<T> {

  private static final ThreadLocal<TSerializer> serializerThreadLocal = new ThreadLocal<TSerializer>() {
    @Override
    protected TSerializer initialValue() {
      return new TSerializer(new TCompactProtocol.Factory());
    }
  };

  private static final ThreadLocal<TDeserializer> deserializerThreadLocal = new ThreadLocal<TDeserializer>() {
    @Override
    protected TDeserializer initialValue() {
      return new TDeserializer(new TCompactProtocol.Factory());
    }
  };

  public WatchedThriftNode(final ZooKeeperPlus zk,
                           final String nodePath,
                           final boolean waitForCreation,
                           final CreateMode createMode,
                           final T initialValue,
                           final T emptyValue) throws KeeperException, InterruptedException {
    super(zk, nodePath, waitForCreation, createMode, initialValue, emptyValue);
  }

  @Override
  protected T decode(byte[] data) {
    if (data == null) {
      return null;
    } else {
      T result = (T) emptyValue.deepCopy();
      try {
        deserializerThreadLocal.get().deserialize(result, data);
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
      try {
        return serializerThreadLocal.get().serialize(v);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public abstract class Updater implements WatchedNodeUpdater<T> {

    private Object returnValue;

    @Override
    public T update(T current) {
      T copy;
      if (current == null) {
        copy = (T) emptyValue.deepCopy();
      } else {
        copy = (T) current.deepCopy();
      }
      updateCopy(copy);
      return copy;
    }

    public abstract void updateCopy(T currentCopy);

    public Object getReturnValue() {
      return returnValue;
    }

    protected void setReturnValue(Object returnValue) {
      this.returnValue = returnValue;
    }
  }
}
