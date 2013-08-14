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

package com.liveramp.hank.util;

public class AtomicLongCollection {

  private final long[] values;

  public AtomicLongCollection(int size) {
    values = new long[size];
  }

  public AtomicLongCollection(int size, long[] initialValues) {
    values = new long[size];
    set(initialValues);
  }

  synchronized public void set(long... newValues) {
    if (values.length != newValues.length) {
      throw new RuntimeException("Expecting " + values.length + " values but was supplied " + newValues.length);
    }
    for (int i = 0; i < newValues.length; ++i) {
      values[i] = newValues[i];
    }
  }

  synchronized public void increment(long... increments) {
    if (values.length != increments.length) {
      throw new RuntimeException("Expecting " + values.length + " increments but was supplied " + increments.length);
    }
    for (int i = 0; i < increments.length; ++i) {
      values[i] += increments[i];
    }
  }

  synchronized public long[] getAsArrayAndSet(long... newValues) {
    long[] result = new long[values.length];
    System.arraycopy(values, 0, result, 0, values.length);
    set(newValues);
    return result;
  }

  synchronized public long[] getAsArray() {
    long[] result = new long[values.length];
    System.arraycopy(values, 0, result, 0, values.length);
    return result;
  }

  synchronized public long get(int index) {
    return values[index];
  }
}
