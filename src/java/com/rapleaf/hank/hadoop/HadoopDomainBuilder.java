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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.log4j.Logger;

public class HadoopDomainBuilder {

  private static final Logger LOG = Logger.getLogger(HadoopDomainBuilder.class);

  public static final void run(String domainName, String configPath, String inputPath, String outputPath) throws IOException {
    LOG.info("Building Hank domain " + domainName + " from input " + inputPath + " and configuration " + configPath);
    buildHankDomain(domainName, inputPath, SequenceFileInputFormat.class, DomainBuilderMapperDefault.class, configPath, outputPath);
  }

  public static final RunningJob buildHankDomain(
      String domainName,
      String inputPath,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends DomainBuilderMapper> mapperClass,
      String hankConfigurationPath,
      String outputPath) throws IOException {
    String hankConfiguration = FileUtils.readFileToString(new File(hankConfigurationPath));
    return JobClient.runJob(createJobConfiguration(domainName, inputPath, inputFormatClass, mapperClass, hankConfiguration, outputPath));
  }

  public static final JobConf createJobConfiguration(String domainName,
      String inputPath,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends Mapper> mapperClass,
      String hankConfiguration,
      String outputPath) {
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
    conf.setOutputFormat(DomainBuilderOutputFormat.class);
    // Partitioner
    conf.setPartitionerClass(DomainBuilderPartitioner.class);
    // Hank specific configuration
    DomainBuilderProperties.set(conf, hankConfiguration, domainName, outputPath);
    return conf;
  }

  public static final void main(String[] args) throws IOException{
    if (args.length != 3) {
      LOG.fatal("Usage: HadoopDomainBuilder <domain name> <config path> <input path> <output_path>");
      System.exit(1);
    }
    run(args[0], args[1], args[2], args[3]);
  }
}
