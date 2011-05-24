package com.rapleaf.hank.hadoop;

import com.rapleaf.hank.storage.VersionType;
import org.apache.hadoop.mapred.JobConf;

import java.util.Properties;

public class DomainBuilderProperties {

  private static final Class<? extends DomainBuilderOutputFormat> DEFAULT_OUTPUT_FORMAT_CLASS = DomainBuilderDefaultOutputFormat.class;

  private final String domainName;
  private final String coordinatorConfiguration;
  private final VersionType versionType;
  private final String outputPath;
  private final Class<? extends DomainBuilderOutputFormat> outputFormatClass;

  // With a default output format
  public DomainBuilderProperties(String domainName,
                                 VersionType versionType,
                                 String coordinatorConfiguration,
                                 String outputPath) {
    this.domainName = domainName;
    this.versionType = versionType;
    this.coordinatorConfiguration = coordinatorConfiguration;
    this.outputPath = outputPath;
    this.outputFormatClass = DEFAULT_OUTPUT_FORMAT_CLASS;
  }

  // With a specific output format
  public DomainBuilderProperties(String domainName,
                                 VersionType versionType,
                                 String coordinatorConfiguration,
                                 String outputPath,
                                 Class<? extends DomainBuilderOutputFormat> outputFormatClass) {
    this.domainName = domainName;
    this.versionType = versionType;
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

  public VersionType getVersionType() {
    return versionType;
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
    // Version type
    properties.setProperty(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_TYPE), versionType.toString());
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
    // Version type
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_TYPE), versionType.toString());
    // Output path
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH),
        getOutputPath());
    return conf;
  }
}
