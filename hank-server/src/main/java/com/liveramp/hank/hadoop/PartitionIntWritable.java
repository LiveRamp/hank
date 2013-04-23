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

package com.liveramp.hank.hadoop;

import org.apache.hadoop.io.IntWritable;

public class PartitionIntWritable extends IntWritable {

  public PartitionIntWritable() {
    super();
  }

  public PartitionIntWritable(int value) {
    super(value);
  }

  @Override
  public boolean equals(Object other) {
    return other != null
        && (PartitionIntWritable.class.equals(other.getClass()))
        && (get() == ((PartitionIntWritable) other).get());
  }

  // Re-implementing hashCode in this way guarantees that the reduce partition computed for that object will be the same
  // as the Hank partition this object represents.
  @Override
  public int hashCode() {
    return get();
  }
}
