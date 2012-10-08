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

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.thirdparty.guava.common.collect.ForwardingMap;
import org.apache.zookeeper.KeeperException;
import org.yaml.snakeyaml.Yaml;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WatchedCompactMap<V> extends ForwardingMap<String, V> implements Map<String, V> {

  public interface ValueEncoder<V> {
    public String encode(V value);

    public V decode(String value);
  }

  private final InternalWatchedCompactMap<V> watchedData;

  public WatchedCompactMap(final ZooKeeperPlus zk,
                           final String nodePath,
                           final boolean waitForCreation,
                           final ValueEncoder<V> valueEncoder) throws KeeperException, InterruptedException {
    this.watchedData = new InternalWatchedCompactMap<V>(zk, nodePath, waitForCreation, valueEncoder);
  }

  @Override
  public V put(final String s, final V v) {
    WatchedNodeUpdaterWithReturnValue<Map<String, V>, V> updater
        = new WatchedNodeUpdaterWithReturnValueImpl<Map<String, V>, V>() {

      @Override
      public Map<String, V> update(Map<String, V> current) {
        Map<String, V> result = new HashMap<String, V>();
        if (current != null) {
          result.putAll(current);
        }
        setReturnValue(result.put(s, v));
        return result;
      }
    };

    try {
      watchedData.update(updater);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
    return updater.getReturnValue();
  }

  @Override
  public V remove(Object key) {
    final Object finalKey = key;
    WatchedNodeUpdaterWithReturnValue<Map<String, V>, V> updater
        = new WatchedNodeUpdaterWithReturnValueImpl<Map<String, V>, V>() {

      @Override
      public Map<String, V> update(Map<String, V> current) {
        Map<String, V> result = new HashMap<String, V>();
        if (current != null) {
          result.putAll(current);
        }
        setReturnValue(result.remove(finalKey));
        return result;
      }
    };

    try {
      watchedData.update(updater);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
    return updater.getReturnValue();
  }

  @Override
  public void putAll(Map<? extends String, ? extends V> map) {
    throw new NotImplementedException();
  }

  @Override
  public void clear() {
    throw new NotImplementedException();
  }

  @Override
  public Set<Entry<String, V>> entrySet() {
    return watchedData.get().entrySet();
  }

  @Override
  protected Map<String, V> delegate() {
    Map<String, V> result = watchedData.get();
    if (result == null) {
      return Collections.emptyMap();
    }
    return result;
  }

  private static class InternalWatchedCompactMap<V> extends WatchedNode<Map<String, V>> {

    private final ValueEncoder<V> valueEncoder;

    public InternalWatchedCompactMap(final ZooKeeperPlus zk,
                                     final String nodePath,
                                     boolean waitForCreation,
                                     ValueEncoder<V> valueEncoder) throws KeeperException, InterruptedException {
      super(zk, nodePath, waitForCreation);
      this.valueEncoder = valueEncoder;
    }

    @Override
    protected Map<String, V> decode(byte[] data) {
      if (data == null) {
        return null;
      }
      Map<String, String> encodedEntries = (Map<String, String>) new Yaml().load(new String(data));
      return decodeEntries(encodedEntries);
    }

    @Override
    protected byte[] encode(Map<String, V> data) {
      Map<String, String> encodedEntries = encodeEntries(data);
      return new Yaml().dump(encodedEntries).getBytes();
    }

    private Map<String, String> encodeEntries(Map<String, V> map) {
      Map<String, String> result = new HashMap<String, String>(map.size());
      for (Map.Entry<String, V> entry : map.entrySet()) {
        result.put(entry.getKey(), this.valueEncoder.encode(entry.getValue()));
      }
      return result;
    }

    private Map<String, V> decodeEntries(Map<String, String> map) {
      Map<String, V> result = new HashMap<String, V>(map.size());
      for (Map.Entry<String, String> entry : map.entrySet()) {
        result.put(entry.getKey(), this.valueEncoder.decode(entry.getValue()));
      }
      return result;
    }
  }
}

