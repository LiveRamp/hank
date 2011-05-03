package com.rapleaf.hank.hadoop;

import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

public class DomainBuilderProperties {

  // For Hadoop M/R jobs
  public static JobConf set(JobConf conf, String configuration, String domainName, String outputPath) {
    conf.set(DomainBuilderOutputFormat.CONF_PARAM_HANK_CONFIGURATION, configuration);
    conf.set(DomainBuilderOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME, domainName);
    conf.set(DomainBuilderOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH, outputPath);
    return conf;
  }

  // For cascading jobs
  public static Properties set(Properties properties, String configuration, String domainName) {
    properties.setProperty(DomainBuilderOutputFormat.CONF_PARAM_HANK_CONFIGURATION, configuration);
    properties.setProperty(DomainBuilderOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME, domainName);
    return properties;
  }
}
