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

package com.rapleaf.hank.cascading;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.hadoop.DomainBuilderDefaultOutputFormat;
import com.rapleaf.hank.util.Bytes;

public class DomainBuilderAssembly extends SubAssembly {

  private static final long serialVersionUID = 1L;
  public static final String PARTITION_FIELD_NAME = "__hank_partition";
  private static final String COMPARABLE_KEY_FIELD_NAME = "__hank_comparableKey";

  public DomainBuilderAssembly (
      Pipe outputPipe,
      String keyFieldName,
      String valueFieldName) {

    // Add partition and comparable key fields
    outputPipe = new Each(outputPipe,
        new Fields(keyFieldName),
        new AddPartitionAndComparableKeyFields(PARTITION_FIELD_NAME, COMPARABLE_KEY_FIELD_NAME),
        new Fields(keyFieldName, valueFieldName, PARTITION_FIELD_NAME, COMPARABLE_KEY_FIELD_NAME));

    // Group by partition id and secondary sort on comparable key
    outputPipe = new GroupBy(outputPipe, new Fields(PARTITION_FIELD_NAME), new Fields(
        COMPARABLE_KEY_FIELD_NAME));

    // Check output
    outputPipe = new Every(outputPipe,
        new Fields(keyFieldName, valueFieldName, PARTITION_FIELD_NAME, COMPARABLE_KEY_FIELD_NAME),
        new DetectDuplicateKeysBuffer(),
        new Fields(keyFieldName, valueFieldName, PARTITION_FIELD_NAME, COMPARABLE_KEY_FIELD_NAME));

    setTails(outputPipe);
  }

  private static class AddPartitionAndComparableKeyFields extends BaseOperation<AddPartitionAndComparableKeyFields> implements Function<AddPartitionAndComparableKeyFields> {

    private static final long serialVersionUID = 1L;
    transient private DomainConfig domainConfig;

    AddPartitionAndComparableKeyFields(String partitionFieldName, String comparableKeyFieldName) {
      super(1, new Fields(partitionFieldName, comparableKeyFieldName));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<AddPartitionAndComparableKeyFields> call) {
      // Load domain config lazily
      try {
        loadDomainConfig(flowProcess);
      } catch (DataNotFoundException e) {
        throw new RuntimeException("Failed to load DomainConfig!", e);
      }
      // Compute partition and comparable key
      TupleEntry tupleEntry = call.getArguments();
      BytesWritable key = (BytesWritable) tupleEntry.get(0);
      ByteBuffer keyByteBuffer = ByteBuffer.wrap(key.getBytes(), 0, key.getLength());
      IntWritable partition = new IntWritable(domainConfig.getPartitioner().partition(keyByteBuffer, domainConfig.getNumParts()));
      ByteBuffer comparableKey =  domainConfig.getStorageEngine().getComparableKey(keyByteBuffer);
      //TODO: avoid this copy
      byte[] comparableKeyBuffer = new byte[comparableKey.remaining()];
      System.arraycopy(comparableKey.array(), comparableKey.position(), comparableKeyBuffer, 0, comparableKey.remaining());
      BytesWritable comparableKeyBytesWritable = new BytesWritable(comparableKeyBuffer);
      // Add partition and comparable key fields
      call.getOutputCollector().add(new Tuple(partition, comparableKeyBytesWritable));
    }

    private void loadDomainConfig(FlowProcess flowProcess) throws DataNotFoundException {
      if (domainConfig == null) {
        Configurator configurator = new CascadingOperationConfigurator(flowProcess);
        String domainName = CascadingOperationConfigurator.getRequiredConfigurationItem(DomainBuilderDefaultOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME, "Hank domain name", flowProcess);
        domainConfig = configurator.getCoordinator().getDomainConfig(domainName);
      }
    }
  }

  private static class DetectDuplicateKeysBuffer extends BaseOperation<DetectDuplicateKeysBuffer> implements Buffer<DetectDuplicateKeysBuffer> {

    public DetectDuplicateKeysBuffer() {
      super(4);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall<DetectDuplicateKeysBuffer> call) {
      Iterator<TupleEntry> it = call.getArgumentsIterator();
      ByteBuffer previousKeyByteBuffer = null;
      while (it.hasNext()) {
        Tuple tuple = it.next().getTuple();
        BytesWritable key = (BytesWritable) tuple.get(0);

        ByteBuffer keyByteBuffer = ByteBuffer.wrap(key.getBytes(), 0, key.getLength());
        if (previousKeyByteBuffer != null && 0 == Bytes.compareBytesUnsigned(keyByteBuffer, previousKeyByteBuffer)) {
          throw new RuntimeException("Keys must be unique but two consecutive keys are equal.");
        }
        previousKeyByteBuffer = Bytes.byteBufferDeepCopy(keyByteBuffer);
        call.getOutputCollector().add(tuple);
      }
    }
  }
}
