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

package com.rapleaf.hank.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.RunWithCoordinator;
import com.rapleaf.hank.coordinator.RunnableWithCoordinator;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import com.rapleaf.hank.hadoop.PartitionIntWritable;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DomainBuilderAssembly extends SubAssembly {

  private static final long serialVersionUID = 1L;
  public static final String PARTITION_FIELD_NAME = "__hank_partition";
  public static final String COMPARABLE_KEY_FIELD_NAME = "__hank_comparable_key";
  private static final String PARTITION_MARKERS_PIPE_NAME_PREFIX = "__hank_partition_markers_for_";
  private static final String SINK_NAME_PREFIX = "__hank_sink_for_";

  public DomainBuilderAssembly(String domainName,
                               Pipe outputPipe,
                               String keyFieldName,
                               String valueFieldName) {
    this(domainName, outputPipe, keyFieldName, valueFieldName, null);
  }

  public DomainBuilderAssembly(String domainName,
                               Pipe outputPipe,
                               String keyFieldName,
                               String valueFieldName,
                               Integer partitionToBuild) {

    Pipe partitionMarkersPipe = new Pipe(getPartitionMarkersPipeName(domainName));

    // Add partition and comparable key fields
    outputPipe = new Each(outputPipe,
        new Fields(keyFieldName),
        new AddPartitionAndComparableKeyFields(domainName, PARTITION_FIELD_NAME, COMPARABLE_KEY_FIELD_NAME),
        new Fields(keyFieldName, valueFieldName, PARTITION_FIELD_NAME, COMPARABLE_KEY_FIELD_NAME));

    if (partitionToBuild != null) {
      outputPipe = new Each(outputPipe, new Fields(PARTITION_FIELD_NAME), new KeepPartitions(partitionToBuild));
      partitionMarkersPipe = new Each(partitionMarkersPipe, new Fields(PARTITION_FIELD_NAME), new KeepPartitions(partitionToBuild));
    }

    // Group by partition id and secondary sort on comparable key
    Pipe tail = new GroupBy(getSinkName(domainName), new Pipe[]{partitionMarkersPipe, outputPipe}, new Fields(PARTITION_FIELD_NAME), new Fields(
        COMPARABLE_KEY_FIELD_NAME));
    setTails(tail);
  }

  private static class KeepPartitions extends BaseOperation implements Filter {

    private final int partitionToKeep;

    KeepPartitions(int partitionToKeep) {
      super(1);
      this.partitionToKeep = partitionToKeep;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      Integer partition = ((IntWritable) filterCall.getArguments().get(0)).get();
      return partition != partitionToKeep;
    }
  }

  private static class AddPartitionAndComparableKeyFields extends BaseOperation<AddPartitionAndComparableKeyFields> implements Function<AddPartitionAndComparableKeyFields> {

    private static final long serialVersionUID = 1L;
    transient private Integer domainNumParts;
    transient private StorageEngine storageEngine;
    transient private Partitioner partitioner;
    private String domainName;

    AddPartitionAndComparableKeyFields(String domainName, String partitionFieldName, String comparableKeyFieldName) {
      super(1, new Fields(partitionFieldName, comparableKeyFieldName));
      this.domainName = domainName;
    }

    public void operate(FlowProcess flowProcess, FunctionCall<AddPartitionAndComparableKeyFields> call) {
      // Load configuration lazily
      loadConfiguration(flowProcess);

      // Compute partition and comparable key
      TupleEntry tupleEntry = call.getArguments();
      BytesWritable key = (BytesWritable) tupleEntry.get(0);
      ByteBuffer keyByteBuffer = ByteBuffer.wrap(key.getBytes(), 0, key.getLength());
      PartitionIntWritable partition = new PartitionIntWritable(partitioner.partition(keyByteBuffer, domainNumParts));
      ByteBuffer comparableKey = storageEngine.getComparableKey(keyByteBuffer);
      byte[] comparableKeyBuffer = new byte[comparableKey.remaining()];
      System.arraycopy(comparableKey.array(), comparableKey.arrayOffset() + comparableKey.position(),
          comparableKeyBuffer, 0, comparableKey.remaining());
      BytesWritable comparableKeyBytesWritable = new BytesWritable(comparableKeyBuffer);
      // Add partition and comparable key fields
      call.getOutputCollector().add(new Tuple(partition, comparableKeyBytesWritable));
    }

    private void loadConfiguration(FlowProcess flowProcess) {
      if (storageEngine == null || partitioner == null) {
        try {
          RunWithCoordinator.run(DomainBuilderProperties.getConfigurator(domainName, flowProcess),
              new RunnableWithCoordinator() {
                @Override
                public void run(Coordinator coordinator) throws IOException {
                  Domain domain = DomainBuilderProperties.getDomain(coordinator, domainName);
                  domainNumParts = domain.getNumParts();
                  storageEngine = domain.getStorageEngine();
                  partitioner = domain.getPartitioner();
                }
              });
        } catch (IOException e) {
          throw new RuntimeException("Failed to load configuration.", e);
        }
      }
    }
  }

  public static String getPartitionMarkersPipeName(String domainName) {
    return PARTITION_MARKERS_PIPE_NAME_PREFIX + domainName;
  }

  public static String getSinkName(String domainName) {
    return SINK_NAME_PREFIX + domainName;
  }
}
