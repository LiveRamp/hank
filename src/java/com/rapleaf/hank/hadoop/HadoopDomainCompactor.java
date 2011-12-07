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
import com.rapleaf.hank.config.DataDirectoriesConfigurator;
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.SimpleDataDirectoriesConfigurator;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.HdfsPartitionRemoteFileOps;
import com.rapleaf.hank.storage.PartitionUpdater;
import com.rapleaf.hank.util.CommandLineChecker;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class HadoopDomainCompactor extends AbstractHadoopDomainBuilder {

  private static final Logger LOG = Logger.getLogger(HadoopDomainCompactor.class);

  private final String hdfsUserName;
  private final String hdfsGroupName;

  public HadoopDomainCompactor() {
    super();
    hdfsUserName = null;
    hdfsGroupName = null;
  }

  public HadoopDomainCompactor(JobConf conf) {
    super(conf);
    hdfsUserName = null;
    hdfsGroupName = null;
  }

  public HadoopDomainCompactor(JobConf conf,
                               String hdfsUserName,
                               String hdfsGroupName) {
    super(conf);
    this.hdfsUserName = hdfsUserName;
    this.hdfsGroupName = hdfsGroupName;
  }

  @Override
  protected void configureJob(JobConf conf) {
    // Input format
    conf.setInputFormat(HadoopDomainCompactorInputFormat.class);

    // Mappers
    conf.setMapperClass(HadoopDomainCompactorMapper.class);
    conf.setMapOutputKeyClass(NullWritable.class);
    conf.setMapOutputValueClass(NullWritable.class);

    // No reducers
    conf.setNumReduceTasks(0);

    // No output
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(NullWritable.class);
  }

  private class HadoopDomainCompactorMapper implements Mapper<Text, IntWritable, NullWritable, NullWritable> {

    private Domain domain;
    private File localTmpOutput;
    private String outputPath;
    private DomainVersion domainVersionToCompact;

    @Override
    public void configure(JobConf conf) {
      domain = DomainBuilderProperties.getDomain(conf);
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
      outputPath = DomainBuilderOutputFormat.getTaskAttemptOutputPath(conf);
      int versionNumberToCompact = DomainCompactorProperties.getVersionNumberToCompact(domain.getName(), conf);
      try {
        domainVersionToCompact = domain.getVersionByNumber(versionNumberToCompact);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load Version " + versionNumberToCompact
            + " of Domain " + domain.getName(), e);
      }
    }

    @Override
    public void map(Text domainName, IntWritable partitionNumber,
                    OutputCollector<NullWritable, NullWritable> stringStringOutputCollector,
                    Reporter reporter) throws IOException {
      LOG.info("Compacting Domain " + domainName.toString()
          + " Version " + domainVersionToCompact.getVersionNumber()
          + " Partition " + partitionNumber.get()
          + " in " + localTmpOutput.getAbsolutePath());
      // Get compacting updater
      DataDirectoriesConfigurator dataDirectoriesConfigurator =
          new SimpleDataDirectoriesConfigurator(localTmpOutput.getAbsolutePath());
      PartitionUpdater compactingUpdater = domain.getStorageEngine()
          .getCompactingUpdater(dataDirectoriesConfigurator, partitionNumber.get());
      if (compactingUpdater == null) {
        throw new RuntimeException("Failed to load compacting updater for domain " + domain.getName()
            + " with storage engine: " + domain.getStorageEngine());
      }
      // Perform compacting update
      compactingUpdater.updateTo(domainVersionToCompact);
      // Copy resulting compacted partition
      domain.getStorageEngine().getCopier(dataDirectoriesConfigurator, partitionNumber.get())
          .copyVersionTo(domainVersionToCompact.getVersionNumber(),
              new HdfsPartitionRemoteFileOps(outputPath, domainVersionToCompact.getVersionNumber(),
                  hdfsUserName, hdfsGroupName));
    }

    @Override
    public void close() throws IOException {
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

    private Domain domain;

    @Override
    public InputSplit[] getSplits(JobConf conf, int ignored) throws IOException {
      Domain domain = getCachedDomain(conf);
      InputSplit[] splits = new InputSplit[domain.getNumParts()];
      for (int partition = 0; partition < domain.getNumParts(); ++partition) {
        splits[partition] = new HadoopDomainCompactorInputSplit(domain.getName(), partition);
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

    private Domain getCachedDomain(JobConf conf) {
      if (domain == null) {
        domain = DomainBuilderProperties.getDomain(conf);
      }
      return domain;
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
        "domain name", "version to compact number", "HDFS user name", "HDFS group name", "config path", "jobjar"},
        HadoopDomainCompactor.class);
    String domainName = args[0];
    Integer versionToCompactNumber = Integer.valueOf(args[1]);
    String hdfsUserName = args[2];
    String hdfsGroupName = args[3];
    CoordinatorConfigurator configurator = new YamlClientConfigurator(args[4]);
    String jobJar = args[5];

    DomainCompactorProperties properties =
        new DomainCompactorProperties(domainName, versionToCompactNumber, configurator);
    JobConf conf = new JobConf();
    conf.setJar(jobJar);
    conf.setJobName(HadoopDomainCompactor.class.getSimpleName()
        + " Domain " + domainName + ", Version " + versionToCompactNumber);
    HadoopDomainCompactor compactor = new HadoopDomainCompactor(conf, hdfsUserName, hdfsGroupName);
    LOG.info("Compacting Hank domain " + domainName + " version " + versionToCompactNumber
        + " with coordinator configuration " + configurator);
    compactor.buildHankDomain(properties);
  }
}
