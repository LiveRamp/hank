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
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.storage.VersionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HadoopDomainBuilder extends AbstractHadoopDomainBuilder {

  private static final Logger LOG = Logger.getLogger(HadoopDomainBuilder.class);

  private final String inputPath;
  private final Class inputFormatClass;
  private final Class mapperClass;

  public HadoopDomainBuilder(final String inputPath, final Class inputFormatClass, final Class mapperClass) {
    this.inputPath = inputPath;
    this.inputFormatClass = inputFormatClass;
    this.mapperClass = mapperClass;
  }

  public void run(String domainName,
                  VersionType versionType,
                  CoordinatorConfigurator configurator,
                  String outputPath) throws IOException {
    LOG.info("Building Hank domain " + domainName + " from input " + inputPath
        + " and coordinator configuration " + configurator);
    DomainBuilderProperties properties = new DomainBuilderProperties(domainName, versionType, configurator, outputPath);
    buildHankDomain(properties);
  }

  // Use a non-default output format
  @Override
  protected void configureJob(int versionNumber, DomainBuilderProperties properties, JobConf conf) {
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
  }

  public static void main(String[] args) throws IOException, InvalidConfigurationException {
    if (args.length != 5) {
      LOG.fatal("Usage: HadoopDomainBuilder <domain name> <'base' or 'delta'> <config path> <input path> <output_path>");
      System.exit(1);
    }
    HadoopDomainBuilder builder = new HadoopDomainBuilder(args[3],
        SequenceFileInputFormat.class,
        DomainBuilderMapperDefault.class);
    builder.run(args[0], VersionType.fromString(args[1]), new YamlClientConfigurator(args[2]), args[4]);
  }
}
