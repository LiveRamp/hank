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

import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.VersionType;
import com.rapleaf.hank.storage.Writer;

// Base class of output formats used to build domains.
public abstract class DomainBuilderOutputFormat implements OutputFormat<KeyAndPartitionWritable, ValueWritable> {

  public static final String CONF_PARAM_HANK_DOMAIN_NAME = "com.rapleaf.hank.output.domain";
  public static final String CONF_PARAM_HANK_CONFIGURATOR = "com.rapleaf.hank.configuration";
  public static final String CONF_PARAM_HANK_VERSION_TYPE = "com.rapleaf.hank.version_type";
  public static final String CONF_PARAM_HANK_OUTPUT_PATH = "com.rapleaf.hank.output.path";
  public static final String CONF_PARAM_HANK_TMP_OUTPUT_PATH = "com.rapleaf.hank.output.tmp_path";
  public static final String CONF_PARAM_HANK_VERSION_NUMBER = "com.rapleaf.hank.output.version_number";

  public static String createConfParamName(String domainName, String confParamName) {
    return domainName + "#" + confParamName;
  }

  // Base class of record writers used to build domains.
  protected static class DomainBuilderRecordWriter implements RecordWriter<KeyAndPartitionWritable, ValueWritable> {

    private Logger LOG = Logger.getLogger(DomainBuilderRecordWriter.class);

    private final Domain domain;
    private final StorageEngine storageEngine;
    private final VersionType versionType;
    private final OutputStreamFactory outputStreamFactory;

    private Writer writer = null;
    private Integer writerPartition = null;
    protected final Set<Integer> writtenPartitions = new HashSet<Integer>();

    DomainBuilderRecordWriter(Domain domain,
        VersionType versionType,
        OutputStreamFactory outputStreamFactory) {
      this.domain = domain;
      this.storageEngine = domain.getStorageEngine();
      this.versionType = versionType;
      this.outputStreamFactory = outputStreamFactory;
    }

    public final void close(Reporter reporter) throws IOException {
      // Close current writer
      closeCurrentWriterIfNeeded();
    }

    public final void write(KeyAndPartitionWritable key, ValueWritable value) throws IOException {
      int partition = key.getPartition();
      // If writing a new partition, get a new writer
      if (writerPartition == null || writerPartition != partition) {
        // Set up new writer
        setNewPartitionWriter(partition);
      }
      // Write record
      writer.write(key.getKey(), value.getAsByteBuffer());
    }

    private void setNewPartitionWriter(int partition) throws IOException {
      LOG.info("Setting up new writer for partition " + partition);
      // First, close current writer
      closeCurrentWriterIfNeeded();
      // Check for existing partitions
      if (writtenPartitions.contains(partition)) {
        throw new RuntimeException("Partition " + partition
            + " has already been written.");
      }
      // Set up new writer
      DomainVersion domainVersion = domain.getOpenedVersion();
      if (domainVersion == null) {
        throw new IOException("There is no version currently open for domain "
            + domain.getName());
      }
      writer = storageEngine.getWriter(outputStreamFactory, partition, domainVersion.getVersionNumber(), versionType.equals(VersionType.BASE));
      writerPartition = partition;
      writtenPartitions.add(partition);
    }

    private void closeCurrentWriterIfNeeded() throws IOException {
      if (writer != null) {
        domain.getOpenedVersion().addPartitionInfo(writerPartition, writer.getNumBytesWritten(), writer.getNumRecordsWritten());
        writer.close();
      }
    }
  }
}
