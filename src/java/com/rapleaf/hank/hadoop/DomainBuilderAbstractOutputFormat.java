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

import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Writer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

// Base class of output formats used to build domains.
public abstract class DomainBuilderAbstractOutputFormat
    implements OutputFormat<KeyAndPartitionWritable, ValueWritable> {

  public static final String CONF_PARAM_HANK_DOMAIN_NAME = "com.rapleaf.hank.output.domain";
  public static final String CONF_PARAM_HANK_CONFIGURATOR = "com.rapleaf.hank.configuration";
  public static final String CONF_PARAM_HANK_OUTPUT_PATH = "com.rapleaf.hank.output.path";
  public static final String CONF_PARAM_HANK_TMP_OUTPUT_PATH = "com.rapleaf.hank.output.tmp_path";
  public static final String CONF_PARAM_HANK_VERSION_NUMBER = "com.rapleaf.hank.output.version_number";

  public static String createConfParamName(String domainName, String confParamName) {
    return domainName + "#" + confParamName;
  }

  @Override
  public void checkOutputSpecs(FileSystem fs, JobConf conf)
      throws IOException {
    String outputPath = getJobOutputPath(conf);

    if (fs == null) {
      fs = FileSystem.get(new Configuration());
    }

    if (fs.exists(new Path(outputPath))) {
      throw new RuntimeException("Output path already exists: " + outputPath);
    }
  }

  protected static String getTaskAttemptOutputPath(JobConf conf) {
    String outputPath = conf.get("mapred.work.output.dir");
    if (outputPath == null) {
      throw new RuntimeException("Path was not set in mapred.work.output.dir");
    }
    return outputPath;
  }

  protected String getJobOutputPath(JobConf conf) {
    String outputPath = conf.get("mapred.output.dir");
    if (outputPath == null) {
      throw new RuntimeException("Path was not set in mapred.output.dir");
    }
    return outputPath;
  }

  // Base class of record writers used to build domains.
  protected abstract static class DomainBuilderRecordWriter implements RecordWriter<KeyAndPartitionWritable, ValueWritable> {

    private Logger LOG = Logger.getLogger(DomainBuilderRecordWriter.class);

    private final CoordinatorConfigurator configurator;
    private final String domainName;
    private final Integer domainVersionNumber;
    private final OutputStreamFactory outputStreamFactory;

    private Domain domain;
    private DomainVersion domainVersion;
    private StorageEngine storageEngine;

    private Writer writer = null;
    private Integer writerPartition = null;
    protected final Set<Integer> writtenPartitions = new HashSet<Integer>();

    DomainBuilderRecordWriter(JobConf conf,
                              OutputStreamFactory outputStreamFactory) throws IOException {
      // Load configuration items
      this.configurator = DomainBuilderProperties.getConfigurator(conf);
      this.domainName = DomainBuilderProperties.getDomainName(conf);
      this.domainVersionNumber = DomainBuilderProperties.getVersionNumber(domainName, conf);
      this.outputStreamFactory = outputStreamFactory;

      RunWithCoordinator.run(configurator,
          new RunnableWithCoordinator() {
            @Override
            public void run(Coordinator coordinator) throws IOException {
              DomainBuilderRecordWriter.this.domain = DomainBuilderProperties.getDomain(coordinator, domainName);
              DomainBuilderRecordWriter.this.domainVersion = DomainBuilderProperties.getDomainVersion(coordinator, domainName, domainVersionNumber);
              DomainBuilderRecordWriter.this.storageEngine = domain.getStorageEngine();
            }
          });
    }

    protected abstract Writer getWriter(StorageEngine storageEngine,
                                        DomainVersion domainVersion,
                                        OutputStreamFactory outputStreamFactory,
                                        int partitionNumber) throws IOException;

    @Override
    public final void close(Reporter reporter) throws IOException {
      // Close current writer
      closeCurrentWriterIfNeeded();
    }

    @Override
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
      // First, close current writer
      closeCurrentWriterIfNeeded();
      LOG.info("Setting up new writer for partition " + partition);
      // Check for existing partitions
      if (writtenPartitions.contains(partition)) {
        throw new RuntimeException("Partition " + partition
            + " has already been written.");
      }
      // Set up new writer
      writer = getWriter(storageEngine,
          domainVersion,
          outputStreamFactory,
          partition);
      writerPartition = partition;
      writtenPartitions.add(partition);
    }

    private void closeCurrentWriterIfNeeded() throws IOException {
      if (writer != null) {
        RunWithCoordinator.run(configurator, new RunnableWithCoordinator() {
          @Override
          public void run(Coordinator coordinator) throws IOException {
            DomainVersion domainVersion = DomainBuilderProperties.getDomainVersion(coordinator,
                domainName,
                domainVersionNumber);
            domainVersion.addPartitionProperties(writerPartition,
                writer.getNumBytesWritten(),
                writer.getNumRecordsWritten());
          }
        });
        LOG.info("Closing current partition writer: " + writer.toString());
        writer.close();
      }
    }
  }
}
