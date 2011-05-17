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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Writer;

// Base class of output formats used to build domains.
public abstract class DomainBuilderOutputFormat implements OutputFormat<KeyAndPartitionWritable, ValueWritable> {

  public static final String CONF_PARAM_HANK_OUTPUT_PATH = "com.rapleaf.hank.output.path";
  public static final String CONF_PARAM_HANK_DOMAIN_NAME = "com.rapleaf.hank.output.domain";
  public static final String CONF_PARAM_HANK_CONFIGURATION = "com.rapleaf.hank.configuration";

  // Base class of record writers used to build domains.
  protected static abstract class DomainBuilderRecordWriter implements RecordWriter<KeyAndPartitionWritable, ValueWritable> {

    private final DomainConfig domainConfig;
    private final StorageEngine storageEngine;
    private final OutputStreamFactory outputStreamFactory;
    private Logger LOG = Logger.getLogger(DomainBuilderRecordWriter.class);

    private Writer writer = null;
    private Integer writerPartition = null;
    protected final Set<Integer> writtenPartitions = new HashSet<Integer>();

    DomainBuilderRecordWriter(DomainConfig domainConfig, OutputStreamFactory outputStreamFactory) {
      this.domainConfig = domainConfig;
      this.storageEngine = domainConfig.getStorageEngine();
      this.outputStreamFactory = outputStreamFactory;
    }

    @Override
    public final void close(Reporter reporter) throws IOException {
      // Close current writer
      closeCurrentWriterIfNeeded();
      finalizeOutput();
    }

    @Override
    public final void write(KeyAndPartitionWritable key, ValueWritable value)
    throws IOException {
      int partition = key.getPartition();
      // If writing a new partition, get a new writer
      if (writerPartition == null ||
          writerPartition != partition) {
        // Set up new writer
        setNewPartitionWriter(partition);
      }
      // Write record
      writer.write(key.getKey(), value.getAsByteBuffer());
    }

    private final void setNewPartitionWriter(int partition) throws IOException {
      LOG.info("Setting up new writer for partition " + partition);
      // First, close current writer
      closeCurrentWriterIfNeeded();
      // Check for existing partitions
      if (writtenPartitions.contains(partition)) {
        throw new RuntimeException("Partition " + partition + " has already been written.");
      }
      // Set up new writer
      // TODO: deal with base/non base
      boolean isBase = true;
      throw new NotImplementedException();
//      writer = storageEngine.getWriter(outputStreamFactory, partition, domainConfig.getVersion(), isBase);
//      writerPartition = partition;
//      writtenPartitions.add(partition);
    }

    private final void closeCurrentWriterIfNeeded() throws IOException {
      if (writer != null) {
        writer.close();
      }
    }

    protected abstract void finalizeOutput() throws IOException;
  }
}
