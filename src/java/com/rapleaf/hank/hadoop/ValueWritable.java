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
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

public class ValueWritable implements WritableComparable<ValueWritable> {

  private BytesWritable value;

  public ValueWritable() {
    value = new BytesWritable();
  }

  public ValueWritable(BytesWritable value) {
    this.value = value;
  }

  public ByteBuffer getAsByteBuffer() {
    return ByteBuffer.wrap(value.getBytes(), 0, value.getLength());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    value.readFields(dataInput);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    value.write(dataOutput);
  }

  @Override
  public int compareTo(ValueWritable other) {
    // Not supposed to be compared
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return "<value: " + value.toString() + ">";
  }
}
