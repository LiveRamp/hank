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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.log4j.Logger;

import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.VersionType;

public class HadoopDomainBuilder {

  private static final Logger LOG = Logger.getLogger(HadoopDomainBuilder.class);

  public static void run(String domainName, VersionType versionType, CoordinatorConfigurator configurator, String inputPath, String outputPath) throws IOException {
    LOG.info("Building Hank domain " + domainName + " from input " + inputPath + " and coordinator configuration " + configurator);
    DomainBuilderProperties properties = new DomainBuilderProperties(domainName, versionType, configurator, outputPath);
    buildHankDomain(inputPath, SequenceFileInputFormat.class, DomainBuilderMapperDefault.class, properties);
  }

  public static void buildHankDomain(
      String inputPath,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends DomainBuilderMapper> mapperClass,
      DomainBuilderProperties properties) throws IOException {
    // Open new version and check for success
    Domain domain = properties.getDomain();
    DomainVersion domainVersion = domain.openNewVersion();
    if (domainVersion == null) {
      throw new IOException("Could not open a new version of domain " + properties.getDomainName());
    }
    // Try to build new version
    JobConf jobConf = createJobConfiguration(inputPath, inputFormatClass, mapperClass, domainVersion.getVersionNumber(), properties);
    try {
      // Set up job
      DomainBuilderOutputCommitter.setupJob(domain.getName(), jobConf);
      // Run job
      JobClient.runJob(jobConf);
      // Commit job
      DomainBuilderOutputCommitter.commitJob(domain.getName(), jobConf);
    } catch (Exception e) {
      // In case of failure, cancel this new version
      domainVersion.cancel();
      // Clean up job
      DomainBuilderOutputCommitter.cleanupJob(domain.getName(), jobConf);
      throw new IOException("Failed at building version " + domainVersion.getVersionNumber() + " of domain " + properties.getDomainName() + ". Cancelling version.", e);
    }
    // Close the new version
    domainVersion.close();
    // Clean up job
    DomainBuilderOutputCommitter.cleanupJob(domain.getName(), jobConf);
  }

  // Use a non-default output format
  public static final JobConf createJobConfiguration(String inputPath,
                                                     Class<? extends InputFormat> inputFormatClass,
                                                     Class<? extends Mapper> mapperClass,
                                                     int versionNumber,
                                                     DomainBuilderProperties properties) {
    JobConf conf = new JobConf();
    // Input specification
    conf.setInputFormat(inputFormatClass);
    FileInputFormat.setInputPaths(conf, inputPath);
    // Mapper class and key/value classes
    conf.setMapperClass(mapperClass);
    conf.setMapOutputKeyClass(KeyAndPartitionWritableComparable.class);
    conf.setMapOutputValueClass(ValueWritable.class);
    // Reducer class and key/value classes
    conf.setReducerClass(DomainBuilderReducer.class);
    conf.setOutputKeyClass(KeyAndPartitionWritable.class);
    conf.setOutputValueClass(ValueWritable.class);
    // Output format
    conf.setOutputFormat(properties.getOutputFormatClass());
    // Output path (set to tmp output path)
    FileOutputFormat.setOutputPath(conf, new Path(properties.getTmpOutputPath(versionNumber)));
    // Partitioner
    conf.setPartitionerClass(DomainBuilderPartitioner.class);
    // Output Committer
    conf.setOutputCommitter(DomainBuilderOutputCommitter.class);
    // Hank specific configuration
    properties.setJobConfProperties(conf, versionNumber);
    return conf;
  }

  public static void main(String[] args) throws IOException, InvalidConfigurationException {
    if (args.length != 5) {
      LOG.fatal("Usage: HadoopDomainBuilder <domain name> <'base' or 'delta'> <config path> <input path> <output_path>");
      System.exit(1);
    }
    run(args[0], VersionType.fromString(args[1]), new YamlClientConfigurator(args[2]), args[3], args[4]);
  }
}
