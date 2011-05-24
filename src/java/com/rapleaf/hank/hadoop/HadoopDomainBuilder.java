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

import com.rapleaf.hank.coordinator.Domain;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class HadoopDomainBuilder {

  private static final Logger LOG = Logger.getLogger(HadoopDomainBuilder.class);

  public static final void run(String domainName, boolean isDelta, String coordinatorConfigurationPath, String inputPath, String outputPath) throws IOException {
    LOG.info("Building Hank domain " + domainName + " from input " + inputPath + " and coordinator configuration " + coordinatorConfigurationPath);
    String coordinatorConfiguration = FileUtils.readFileToString(new File(coordinatorConfigurationPath));
    DomainBuilderProperties properties = new DomainBuilderProperties(domainName, isDelta, coordinatorConfiguration, outputPath);
    buildHankDomain(inputPath, SequenceFileInputFormat.class, DomainBuilderMapperDefault.class, properties);
  }

  public static final void buildHankDomain(
      String inputPath,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends DomainBuilderMapper> mapperClass,
      DomainBuilderProperties properties) throws IOException {
    // Open new version and check for success
    Domain domainConfig = DomainBuilderPropertiesConfigurator.getDomainConfig(properties);
    Integer version = domainConfig.openNewVersion();
    if (version == null) {
      throw new IOException("Could not open a new version of domain " + properties.getDomainName());
    }
    // Try to build new version
    try {
      JobClient.runJob(createJobConfiguration(inputPath, inputFormatClass, mapperClass, properties));
    } catch (Exception e) {
      // In case of failure, cancel this new version
      domainConfig.cancelNewVersion();
      throw new IOException("Failed at building version " + version + " of domain " + properties.getDomainName() + ". Cancelling version.", e);
    }
    // Close the new version
    domainConfig.closeNewVersion();
  }

  // Use a non-default output format
  public static final JobConf createJobConfiguration(String inputPath,
                                                     Class<? extends InputFormat> inputFormatClass,
                                                     Class<? extends Mapper> mapperClass,
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
    // Partitioner
    conf.setPartitionerClass(DomainBuilderPartitioner.class);
    // Hank specific configuration
    properties.setJobConfProperties(conf);
    return conf;
  }

  public static final void main(String[] args) throws IOException {
    if (args.length != 5) {
      LOG.fatal("Usage: HadoopDomainBuilder <domain name> <config path> <input path> <output_path> ['delta']");
      System.exit(1);
    }
    boolean isDelta = args[4].equals("delta");
    run(args[0], isDelta, args[1], args[2], args[3]);
  }
}
