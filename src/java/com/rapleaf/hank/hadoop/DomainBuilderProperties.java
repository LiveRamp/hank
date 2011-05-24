package com.rapleaf.hank.hadoop;

import org.apache.hadoop.mapred.JobConf;

import java.util.Properties;

public class DomainBuilderProperties {

  private static final Class<? extends DomainBuilderOutputFormat> DEFAULT_OUTPUT_FORMAT_CLASS = DomainBuilderDefaultOutputFormat.class;

  private final String domainName;
  private final String coordinatorConfiguration;
  private final boolean isDelta;
  private final String outputPath;
  private final Class<? extends DomainBuilderOutputFormat> outputFormatClass;

  // With a default output format
  public DomainBuilderProperties(String domainName,
                                 boolean isDelta,
                                 String coordinatorConfiguration,
                                 String outputPath) {
    this.domainName = domainName;
    this.isDelta = isDelta;
    this.coordinatorConfiguration = coordinatorConfiguration;
    this.outputPath = outputPath;
    this.outputFormatClass = DEFAULT_OUTPUT_FORMAT_CLASS;
  }

  // With a specific output format
  public DomainBuilderProperties(String domainName,
                                 boolean isDelta,
                                 String coordinatorConfiguration,
                                 String outputPath,
                                 Class<? extends DomainBuilderOutputFormat> outputFormatClass) {
    this.domainName = domainName;
    this.isDelta = isDelta;
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

  public boolean getIsDelta() {
    return isDelta;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public Class<? extends DomainBuilderOutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  // To configure cascading jobs
  public Properties setCascadingProperties(Properties properties) {
    // Domain name is set in DomainBuilderTap
    // Configuration
    properties.setProperty(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_CONFIGURATION), getCoordinatorConfiguration());
    // Is delta
    properties.setProperty(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_IS_DELTA), Boolean.toString(isDelta));
    // Output path is set in DomainBuilderTap
    return properties;
  }

  // To configure Hadoop M/R jobs
  public JobConf setJobConfProperties(JobConf conf) {
    // Domain name
    conf.set(DomainBuilderOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME, getDomainName());
    // Configuration
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_CONFIGURATION),
        getCoordinatorConfiguration());
    // Is delta
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_IS_DELTA), Boolean.toString(isDelta));
    // Output path
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH),
        getOutputPath());
    return conf;
  }
}
