package com.rapleaf.hank.hadoop;

import org.apache.hadoop.mapred.JobConf;

import java.util.Properties;

public class DomainBuilderProperties {

  private static final Class<? extends DomainBuilderOutputFormat> DEFAULT_OUTPUT_FORMAT_CLASS = DomainBuilderDefaultOutputFormat.class;

  private final String domainName;
  private final String coordinatorConfiguration;
  private final String outputPath;
  private final Class<? extends DomainBuilderOutputFormat> outputFormatClass;

  public DomainBuilderProperties(String domainName, String coordinatorConfiguration, String outputPath) {
    this.domainName = domainName;
    this.coordinatorConfiguration = coordinatorConfiguration;
    this.outputPath = outputPath;
    this.outputFormatClass = DEFAULT_OUTPUT_FORMAT_CLASS;
  }

  public DomainBuilderProperties(String domainName, String coordinatorConfiguration, String outputPath, Class<? extends DomainBuilderOutputFormat> outputFormatClass) {
    this.domainName = domainName;
    this.coordinatorConfiguration = coordinatorConfiguration;
    this.outputPath = outputPath;
    this.outputFormatClass = outputFormatClass;
  }

  public String getDomainName() {
    return domainName;
  }

  public String getCoordinatorConfiguration() {
    return coordinatorConfiguration;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public Class<? extends DomainBuilderOutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  // To configure cascading jobs
  public Properties setCascadingProperties(Properties properties) {
    // Domain Name is set in DomainBuilderTap
    properties.setProperty(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderDefaultOutputFormat.CONF_PARAM_HANK_CONFIGURATION), getCoordinatorConfiguration());
    // Output Path is set in DomainBuilderTap
    return properties;
  }

  // To configure Hadoop M/R jobs
  public JobConf setJobConfProperties(JobConf conf) {
    conf.set(DomainBuilderDefaultOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME, getDomainName());
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderDefaultOutputFormat.CONF_PARAM_HANK_CONFIGURATION),
        getCoordinatorConfiguration());
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderDefaultOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH),
        getOutputPath());
    return conf;
  }
}
