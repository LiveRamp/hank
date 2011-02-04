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
package com.rapleaf.tiamat.cascading;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.rapleaf.tiamat.config.DomainConfig;
import com.rapleaf.tiamat.partitioner.Partitioner;

public class TiamatAssembly extends SubAssembly {

  public static final String PARTITION_FIELD_NAME = "__partition";
  private static final long serialVersionUID = 1L;
  private static final String SORTABLE_KEY_FIELD_NAME = "__sortableKey";

  public TiamatAssembly(DomainConfig domainConfig, Pipe outputPipe, String keyFieldName,
      String valueFieldName) {

    // Append partition id
    outputPipe = new Each(outputPipe,
        new Fields(keyFieldName),
        new ComputePartition(domainConfig.getPartitioner(), PARTITION_FIELD_NAME),
        new Fields(PARTITION_FIELD_NAME, keyFieldName, valueFieldName));

    // Create sortable key
    outputPipe = new Each(outputPipe, new Fields(keyFieldName), new CreateSortableKey(SORTABLE_KEY_FIELD_NAME),
        new Fields(PARTITION_FIELD_NAME, keyFieldName, valueFieldName, SORTABLE_KEY_FIELD_NAME));

    // Group by partition id and secondary sort on SORTABLE_KEY_FIELD
    outputPipe = new GroupBy(outputPipe, new Fields(PARTITION_FIELD_NAME), new Fields(
        SORTABLE_KEY_FIELD_NAME));

    setTails(outputPipe);
  }

  private static class ComputePartition extends BaseOperation implements Function {

    private Partitioner partitioner;

    ComputePartition(Partitioner partitioner, String partitionFieldName) {
      super(1, new Fields(partitionFieldName));
      this.partitioner = partitioner;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall call) {
      TupleEntry tupleEntry = call.getArguments();
      BytesWritable key = (BytesWritable) tupleEntry.get(0);
      int partition = partitioner.partition(ByteBuffer.wrap(key.getBytes(), 0, key.getLength()));
      call.getOutputCollector().add(new Tuple(partition));
    }
  }

  private static class CreateSortableKey extends BaseOperation implements Function {

    CreateSortableKey(String sortableKeyFieldName) {
      super(1, new Fields(sortableKeyFieldName));
    }

    @Override
    public void operate(FlowProcess process, FunctionCall call) {
      TupleEntry tupleEntry = call.getArguments();
      BytesWritable key = (BytesWritable) tupleEntry.get(0);
      // TODO: compute the actual hash to sort on
      BytesWritable sortableKey = key;
      call.getOutputCollector().add(new Tuple(sortableKey));
    }
  }
}
