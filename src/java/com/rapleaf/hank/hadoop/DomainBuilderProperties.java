package com.rapleaf.hank.hadoop;

import com.rapleaf.hank.storage.VersionType;
import org.apache.hadoop.mapred.JobConf;

import java.util.Properties;

public class DomainBuilderProperties {

  private static final String TMP_OUTPUT_PATH = "_temporary/";

  private static final Class<? extends DomainBuilderOutputFormat> DEFAULT_OUTPUT_FORMAT_CLASS = DomainBuilderDefaultOutputFormat.class;

  private final String domainName;
  private final String coordinatorConfiguration;
  private final VersionType versionType;
  private final String outputPath;
  private final Class<? extends DomainBuilderOutputFormat> outputFormatClass;

  // With a default output format
  // Get output path from the Coordinator
  public DomainBuilderProperties(String domainName,
                                 VersionType versionType,
                                 String coordinatorConfiguration) {
    this.domainName = domainName;
    this.versionType = versionType;
    this.coordinatorConfiguration = coordinatorConfiguration;
    this.outputPath = DomainBuilderConfigurator.getRemoteDomainRoot(domainName, coordinatorConfiguration);
    this.outputFormatClass = DEFAULT_OUTPUT_FORMAT_CLASS;
  }

  // With a specific output format
  // Get output path from the Coordinator
  public DomainBuilderProperties(String domainName,
                                 VersionType versionType,
                                 String coordinatorConfiguration,
                                 Class<? extends DomainBuilderOutputFormat> outputFormatClass) {
    this.domainName = domainName;
    this.versionType = versionType;
    this.coordinatorConfiguration = coordinatorConfiguration;
    this.outputPath = DomainBuilderConfigurator.getRemoteDomainRoot(domainName, coordinatorConfiguration);
    this.outputFormatClass = outputFormatClass;
  }

  // With a default output format
  // With a specific output path
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
  // With a specific output path
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

  public String getTmpOutputPath(int versionNumber) {
    return outputPath + "/" + TMP_OUTPUT_PATH + "version-" + versionNumber + "/";
  }

  public Class<? extends DomainBuilderOutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  // To configure cascading jobs
  public Properties setCascadingProperties(Properties properties, int versionNumber) {

    // Note: Domain name is set locally in DomainBuilderTap to deal with Cascading
    // jobs building multiple domains.

    // Configuration
    properties.setProperty(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_COORDINATOR_CONFIGURATION),
        getCoordinatorConfiguration());
    // Version type
    properties.setProperty(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_TYPE),
        versionType.toString());
    // Output Path
    properties.setProperty(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH), outputPath);
    // Tmp output path
    properties.setProperty(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_TMP_OUTPUT_PATH),
        getTmpOutputPath(versionNumber));
    // Version Number
    properties.setProperty(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_NUMBER),
        Integer.toString(versionNumber));
    return properties;
  }

  // To configure Hadoop MapReduce jobs
  public JobConf setJobConfProperties(JobConf conf, int versionNumber) {
    // Domain name
    conf.set(DomainBuilderOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME, getDomainName());
    // Configuration
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_COORDINATOR_CONFIGURATION),
        getCoordinatorConfiguration());
    // Version type
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_TYPE),
        versionType.toString());
    // Output path
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH),
        getOutputPath());
    // Tmp output path
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_TMP_OUTPUT_PATH),
        getTmpOutputPath(versionNumber));
    // Version Number
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_NUMBER),
        Integer.toString(versionNumber));
    return conf;
  }

  public static String getDomainName(JobConf conf) {
    return JobConfConfigurator.getRequiredConfigurationItem(DomainBuilderOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME,
        "Hank domain name", conf);
  }

  public static String getCoordinatorConfiguration(String domainName, JobConf conf) {
    return JobConfConfigurator.getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_COORDINATOR_CONFIGURATION),
        "Hank coordinator configuration", conf);
  }

  public static VersionType getVersionType(String domainName, JobConf conf) {
    return VersionType.valueOf(JobConfConfigurator.getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_TYPE),
        "Hank version type (base or delta)", conf));
  }

  public static String getOutputPath(String domainName, JobConf conf) {
    return JobConfConfigurator.getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH),
        "Hank output path", conf);
  }

  public static String getTmpOutputPath(String domainName, JobConf conf) {
    return JobConfConfigurator.getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_TMP_OUTPUT_PATH),
        "Hank temporary output path", conf);
  }

  public static Integer getVersionNumber(String domainName, JobConf conf) {
    return Integer.valueOf(JobConfConfigurator.getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_NUMBER),
        "Hank temporary output path", conf));
  }
}
