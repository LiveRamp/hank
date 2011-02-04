/**
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

public class TiamatRecordWritable implements WritableComparable{

  private BytesWritable key;
  private BytesWritable value;

  public TiamatRecordWritable() {
    key = new BytesWritable();
    value = new BytesWritable();
  }

  public TiamatRecordWritable(byte[] key, byte[] value) {
    this.key = new BytesWritable(key);
    this.value = new BytesWritable(value);
  }

  public TiamatRecordWritable(BytesWritable key, BytesWritable value) {
    this.key = key;
    this.value = value;
  }

  public byte[] getKey() {
    return key.getBytes();
  }

  public byte[] getValue() {
    return value.getBytes();
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    key.readFields(dataInput);
    value.readFields(dataInput);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    key.write(dataOutput);
    value.write(dataOutput);
  }

  @Override
  public int compareTo(Object otherO) {
    TiamatRecordWritable other = (TiamatRecordWritable) otherO;
    int ret = key.compareTo(other.key);
    if (ret == 0) {
      ret = value.compareTo(other.value);
    }
    return ret;
  }

  @Override
  public String toString() {
    return "<" + key.toString() + ", " + value.toString() + ">";
  }
}
