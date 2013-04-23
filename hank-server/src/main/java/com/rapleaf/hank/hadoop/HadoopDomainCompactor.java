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

package com.rapleaf.hank.hadoop;

import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.config.DataDirectoriesConfigurator;
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.SimpleDataDirectoriesConfigurator;
import com.rapleaf.hank.config.yaml.YamlCoordinatorConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.storage.Compactor;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.hank.util.CommandLineChecker;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class HadoopDomainCompactor extends AbstractHadoopDomainBuilder {

  private static final Logger LOG = Logger.getLogger(HadoopDomainCompactor.class);

  public HadoopDomainCompactor() {
    super();
  }

  public HadoopDomainCompactor(JobConf conf) {
    super(conf);
  }

  @Override
  protected void configureJob(JobConf conf) {
    // Input format
    conf.setInputFormat(HadoopDomainCompactorInputFormat.class);

    // Mappers
    conf.setMapperClass(HadoopDomainCompactorMapper.class);
    conf.setMapOutputKeyClass(KeyAndPartitionWritable.class);
    conf.setMapOutputValueClass(ValueWritable.class);

    // No reducers
    conf.setNumReduceTasks(0);

    // Output
    conf.setOutputKeyClass(KeyAndPartitionWritable.class);
    conf.setOutputValueClass(ValueWritable.class);
  }

  private static class HadoopDomainCompactorMapper implements Mapper<Text, IntWritable,
      KeyAndPartitionWritable, ValueWritable> {

    //private Domain domain;
    private DomainVersion domainVersionToCompact;
    private StorageEngine storageEngine;
    private File localTmpOutput;
    private Coordinator coordinator;

    @Override
    public void configure(JobConf conf) {
      // Create unique local directory
      String uniqueString = UUID.randomUUID().toString();
      String localTmpOutputPath;
      try {
        localTmpOutputPath = conf.getLocalPath(uniqueString).toString();
      } catch (IOException e) {
        throw new RuntimeException("Failed to determine local temporary output directory", e);
      }
      localTmpOutput = new File(localTmpOutputPath);
      if (localTmpOutput.exists() || !localTmpOutput.mkdirs()) {
        throw new RuntimeException("Failed to initialize local temporary output directory " + localTmpOutputPath);
      }
      // Load configuration items
      final String domainName = DomainBuilderProperties.getDomainName(conf);
      final int versionNumberToCompact = DomainCompactorProperties.getVersionNumberToCompact(domainName, conf);

      // Create Coordinator
      coordinator = RunWithCoordinator.createCoordinator(DomainBuilderProperties.getConfigurator(conf));

      // Determine version to compact
      try {
        Domain domain = DomainBuilderProperties.getDomain(coordinator, domainName);
        HadoopDomainCompactorMapper.this.storageEngine = domain.getStorageEngine();
        HadoopDomainCompactorMapper.this.domainVersionToCompact =
            DomainBuilderProperties.getDomainVersion(coordinator, domainName, versionNumberToCompact);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load configuration.", e);
      }
    }

    @Override
    public void map(Text domainName, IntWritable partitionNumber,
                    OutputCollector<KeyAndPartitionWritable, ValueWritable> outputCollector,
                    Reporter reporter) throws IOException {
      LOG.info("Compacting Domain " + domainName.toString()
          + " Version " + domainVersionToCompact.getVersionNumber()
          + " Partition " + partitionNumber.get()
          + " in " + localTmpOutput.getAbsolutePath());
      // Get compacting updater
      DataDirectoriesConfigurator dataDirectoriesConfigurator =
          new SimpleDataDirectoriesConfigurator(localTmpOutput.getAbsolutePath());
      Compactor compactor = storageEngine.getCompactor(dataDirectoriesConfigurator, partitionNumber.get());
      if (compactor == null) {
        throw new RuntimeException("Failed to load compacting updater for domain " + domainName
            + " with storage engine: " + storageEngine);
      }
      // Close coordinator when possible
      compactor.closeCoordinatorOpportunistically(coordinator);
      // Perform compaction
      compactor.compact(domainVersionToCompact, new OutputCollectorWriter(reporter, partitionNumber, outputCollector));
    }

    @Override
    public void close() throws IOException {
      LOG.info("Deleting local temporary directory " + localTmpOutput.getAbsolutePath());
      FileUtils.deleteDirectory(localTmpOutput);
      if (coordinator != null) {
        coordinator.close();
      }
    }
  }

  private static class HadoopDomainCompactorInputSplit implements InputSplit {

    private String domainName;
    private int partitionNumber;

    public HadoopDomainCompactorInputSplit() {
    }

    public HadoopDomainCompactorInputSplit(String domainName, int partitionNumber) {
      this.domainName = domainName;
      this.partitionNumber = partitionNumber;
    }

    @Override
    public long getLength() throws IOException {
      return 1;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[]{};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      WritableUtils.writeString(dataOutput, domainName);
      WritableUtils.writeVInt(dataOutput, partitionNumber);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      domainName = WritableUtils.readString(dataInput);
      partitionNumber = WritableUtils.readVInt(dataInput);
    }

    public String getDomainName() {
      return domainName;
    }

    public int getPartitionNumber() {
      return partitionNumber;
    }
  }

  private static class HadoopDomainCompactorInputFormat implements InputFormat<Text, IntWritable> {

    private int domainNumParts;

    @Override
    public InputSplit[] getSplits(JobConf conf, int ignored) throws IOException {
      final String domainName = DomainBuilderProperties.getDomainName(conf);
      RunWithCoordinator.run(DomainBuilderProperties.getConfigurator(conf), new RunnableWithCoordinator() {
        @Override
        public void run(Coordinator coordinator) throws IOException {
          Domain domain = DomainBuilderProperties.getDomain(coordinator, domainName);
          domainNumParts = domain.getNumParts();
        }
      });
      InputSplit[] splits = new InputSplit[domainNumParts];
      for (int partition = 0; partition < domainNumParts; ++partition) {
        splits[partition] = new HadoopDomainCompactorInputSplit(domainName, partition);
      }
      return splits;
    }

    @Override
    public RecordReader<Text, IntWritable> getRecordReader(InputSplit inputSplit,
                                                           JobConf conf,
                                                           Reporter reporter) throws IOException {
      HadoopDomainCompactorInputSplit split = (HadoopDomainCompactorInputSplit) inputSplit;
      return new HadoopDomainCompactorRecordReader(split);
    }
  }

  private static class HadoopDomainCompactorRecordReader implements RecordReader<Text, IntWritable> {

    private final HadoopDomainCompactorInputSplit split;
    private boolean done = false;

    public HadoopDomainCompactorRecordReader(HadoopDomainCompactorInputSplit split) {
      this.split = split;
    }

    @Override
    public boolean next(Text domainName, IntWritable partitionNumber) throws IOException {
      if (done) {
        return false;
      } else {
        domainName.set(split.getDomainName());
        partitionNumber.set(split.getPartitionNumber());
        done = true;
        return true;
      }
    }

    @Override
    public Text createKey() {
      return new Text();
    }

    @Override
    public IntWritable createValue() {
      return new IntWritable();
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      if (done) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  public static void main(String[] args) throws IOException, InvalidConfigurationException {
    CommandLineChecker.check(args, new String[]{
        "domain name", "version to compact number", "mapred.task.timeout", "config path", "jobjar"},
        HadoopDomainCompactor.class);
    String domainName = args[0];
    Integer versionToCompactNumber = Integer.valueOf(args[1]);
    Integer mapredTaskTimeout = Integer.valueOf(args[2]);
    CoordinatorConfigurator configurator = new YamlCoordinatorConfigurator(args[3]);
    String jobJar = args[4];

    DomainCompactorProperties properties =
        new DomainCompactorProperties(domainName, versionToCompactNumber, configurator);
    JobConf conf = new JobConf();
    conf.setJar(jobJar);
    conf.set("mapred.task.timeout", mapredTaskTimeout.toString());
    conf.setJobName(HadoopDomainCompactor.class.getSimpleName()
        + " Domain " + domainName + ", Version " + versionToCompactNumber);
    HadoopDomainCompactor compactor = new HadoopDomainCompactor(conf);
    LOG.info("Compacting Hank domain " + domainName + " version " + versionToCompactNumber
        + " with coordinator configuration " + configurator);
    compactor.buildHankDomain(properties, new IncrementalDomainVersionProperties.Base("Version "
        + versionToCompactNumber + " compacted"));
  }
}
