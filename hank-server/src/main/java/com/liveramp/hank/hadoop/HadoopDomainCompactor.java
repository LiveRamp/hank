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

import com.google.common.collect.Lists;
import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.DataDirectoriesConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.SimpleDataDirectoriesConfigurator;
import com.liveramp.hank.config.yaml.YamlCoordinatorConfigurator;
import com.liveramp.hank.coordinator.*;
import com.liveramp.hank.storage.Compactor;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.storage.incremental.IncrementalStorageEngine;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlan;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlanner;
import com.liveramp.hank.util.CommandLineChecker;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.*;

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
    private String[] locations;

    public HadoopDomainCompactorInputSplit() {
    }

    public HadoopDomainCompactorInputSplit(String domainName, int partitionNumber, String[] locations) {
      this.domainName = domainName;
      this.partitionNumber = partitionNumber;
      this.locations = locations;
    }

    @Override
    public long getLength() throws IOException {
      return 1;
    }

    @Override
    public String[] getLocations() throws IOException {
      return locations;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      WritableUtils.writeString(dataOutput, domainName);
      WritableUtils.writeVInt(dataOutput, partitionNumber);
      WritableUtils.writeStringArray(dataOutput, locations);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      domainName = WritableUtils.readString(dataInput);
      partitionNumber = WritableUtils.readVInt(dataInput);
      locations = WritableUtils.readStringArray(dataInput);
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
    private DomainVersion domainVersionToCompact;

    private static final int MAX_BLOCK_LOCATIONS_PER_SPLIT = 6;

    @Override
    public InputSplit[] getSplits(final JobConf conf, int ignored) throws IOException {
      final String domainName = DomainBuilderProperties.getDomainName(conf);
      RunWithCoordinator.run(DomainBuilderProperties.getConfigurator(conf), new RunnableWithCoordinator() {
        @Override
        public void run(Coordinator coordinator) throws IOException {
          domain = DomainBuilderProperties.getDomain(coordinator, domainName);
          domainVersionToCompact = domain.getVersion(DomainCompactorProperties.getVersionNumberToCompact(domainName, conf));
        }
      });

      final int domainNumParts = domain.getNumParts();
      final StorageEngine storageEngine = domain.getStorageEngine();
      final InputSplit[] splits = new InputSplit[domainNumParts];

      // Create splits
      for (int partition = 0; partition < domainNumParts; ++partition) {

        // Compute remote partition file paths for this split if possible
        List<String> locations = new ArrayList<String>();
        if (storageEngine instanceof IncrementalStorageEngine) {
          IncrementalUpdatePlanner updatePlanner = ((IncrementalStorageEngine) storageEngine).getUpdatePlanner(domain);
          IncrementalUpdatePlan updatePlan = updatePlanner.computeUpdatePlan(domainVersionToCompact);
          List<String> paths = updatePlanner.getRemotePartitionFilePaths(updatePlan, storageEngine.getPartitionRemoteFileOps(partition));
          locations = computeOptimalHosts(conf, paths);
        }

        splits[partition] = new HadoopDomainCompactorInputSplit(domainName, partition, locations.toArray(new String[locations.size()]));
      }
      return splits;
    }

    private List<String> computeOptimalHosts(JobConf jobConf, Iterable<String> resourceNames) {
      if (jobConf == null) {
        return Lists.newArrayList();
      }

      try {
        Map<String, Long> numBytesPerHost = new HashMap<String, Long>();
        for (String resource : resourceNames) {
          addToHostsAndSizes(numBytesPerHost, resource, jobConf);
        }

        final int numHosts = Math.min(numBytesPerHost.size(), MAX_BLOCK_LOCATIONS_PER_SPLIT);

        List<String> sortedHosts = scoreAndSortHosts(numBytesPerHost, numHosts);
        return sortedHosts;
      } catch (IOException e) {
        return Lists.newArrayList();
      }
    }

    private List<String> scoreAndSortHosts(Map<String, Long> numBytesPerHost, int numHosts) {
      List<ScoredHost> scoredHosts = new ArrayList<ScoredHost>(numBytesPerHost.size());
      for (Map.Entry<String, Long> entry : numBytesPerHost.entrySet()) {
        scoredHosts.add(new ScoredHost(entry.getKey(), entry.getValue()));
      }

      Collections.sort(scoredHosts);
      List<String> sortedHostNames = Lists.newArrayList();

      for (int i = 0; i < numHosts; i++) {
        sortedHostNames.add(scoredHosts.get(i).hostname);
      }
      return sortedHostNames;

    }

    private void addToHostsAndSizes(Map<String, Long> numBytesPerHost, String location, JobConf jobConf) throws IOException {
      Path path = new Path(location);
      FileSystem fileSystem = path.getFileSystem(jobConf);
      FileStatus status = fileSystem.getFileStatus(path);
      BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(status, 0, status.getLen());

      if (blockLocations != null) {
        for (BlockLocation blockLocation : blockLocations) {
          Long size = blockLocation.getLength();
          for (String host : blockLocation.getHosts()) {
            incrNumBytesPerHost(numBytesPerHost, host, size);
          }
        }
      }
    }

    private void incrNumBytesPerHost(Map<String, Long> numBytesPerHost, String host, Long numBytes) {
      if (!numBytesPerHost.containsKey(host)) {
        numBytesPerHost.put(host, numBytes);
      } else {
        numBytesPerHost.put(host, numBytesPerHost.get(host) + numBytes);
      }
    }

    @Override
    public RecordReader<Text, IntWritable> getRecordReader(InputSplit inputSplit,
                                                           JobConf conf,
                                                           Reporter reporter) throws IOException {
      HadoopDomainCompactorInputSplit split = (HadoopDomainCompactorInputSplit) inputSplit;
      return new HadoopDomainCompactorRecordReader(split);
    }

    private static class ScoredHost implements Comparable<ScoredHost> {
      public String hostname;
      public long numBytesInHost;

      public ScoredHost(String hostname, long numBytesInHost) {
        this.hostname = hostname;
        this.numBytesInHost = numBytesInHost;
      }

      @Override
      public int compareTo(ScoredHost scoredHost) {
        int bytesCmp = compareNumBytes(numBytesInHost, scoredHost.numBytesInHost);

        if (bytesCmp == 0) {
          return hostname.compareTo(scoredHost.hostname);
        }

        return bytesCmp;
      }

      // sort in reverse order by number of bytes in the host
      private static int compareNumBytes(long thisCount, long thatCount) {
        return Long.valueOf(thatCount).compareTo(thisCount);
      }
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
