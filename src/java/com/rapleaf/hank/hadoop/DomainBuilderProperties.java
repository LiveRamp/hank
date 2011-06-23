package com.rapleaf.hank.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowProcess;

import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.storage.VersionType;

public class DomainBuilderProperties {

  private static final String TMP_OUTPUT_PATH = "_temporary/";

  public static final String REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION = "remote_domain_root";

  private static final Class<? extends DomainBuilderOutputFormat> DEFAULT_OUTPUT_FORMAT_CLASS = DomainBuilderDefaultOutputFormat.class;

  private final String domainName;
  private final Configurator configurator;
  private final VersionType versionType;
  private final String outputPath;
  private final Class<? extends DomainBuilderOutputFormat> outputFormatClass;

  // With a default output format
  // Get output path from the Coordinator
  public DomainBuilderProperties(String domainName,
                                 VersionType versionType,
                                 Configurator configurator) {
    this.domainName = domainName;
    this.versionType = versionType;
    this.configurator = configurator;
    this.outputPath = getRemoteDomainRoot(domainName, configurator);
    this.outputFormatClass = DEFAULT_OUTPUT_FORMAT_CLASS;
  }

  // With a specific output format
  // Get output path from the Coordinator
  public DomainBuilderProperties(String domainName,
                                 VersionType versionType,
                                 Configurator configurator,
                                 Class<? extends DomainBuilderOutputFormat> outputFormatClass) {
    this.domainName = domainName;
    this.versionType = versionType;
    this.configurator = configurator;
    this.outputPath = getRemoteDomainRoot(domainName, configurator);
    this.outputFormatClass = outputFormatClass;
  }

  // With a default output format
  // With a specific output path
  public DomainBuilderProperties(String domainName,
                                 VersionType versionType,
                                 Configurator configurator,
                                 String outputPath) {
    this.domainName = domainName;
    this.versionType = versionType;
    this.configurator = configurator;
    this.outputPath = outputPath;
    this.outputFormatClass = DEFAULT_OUTPUT_FORMAT_CLASS;
  }

  // With a specific output format
  // With a specific output path
  public DomainBuilderProperties(String domainName,
                                 VersionType versionType,
                                 Configurator configurator,
                                 String outputPath,
                                 Class<? extends DomainBuilderOutputFormat> outputFormatClass) {
    this.domainName = domainName;
    this.versionType = versionType;
    this.configurator = configurator;
    this.outputPath = outputPath;
    this.outputFormatClass = outputFormatClass;
  }

  public Domain getDomain() {
    return getConfigurator().getCoordinator().getDomain(domainName);
  }

  public String getDomainName() {
    return domainName;
  }

  public Configurator getConfigurator() {
    return configurator;
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
        DomainBuilderOutputFormat.CONF_PARAM_HANK_CONFIGURATOR),
        buildConfigurationString(getConfigurator()));
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
        DomainBuilderOutputFormat.CONF_PARAM_HANK_CONFIGURATOR),
        buildConfigurationString(getConfigurator()));
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

  // TODO: maybe refactor and move the flow process stuff to the cascading package

  // FlowProcess
  public static Domain getDomain(String domainName, FlowProcess flowProcess) {
    return getConfigurator(domainName, flowProcess).getCoordinator().getDomain(domainName);
  }

  public static Configurator getConfigurator(String domainName, FlowProcess flowProcess) {
    String configurationItem = DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_CONFIGURATOR);
    String configuratorString = getRequiredConfigurationItem(configurationItem,
        "Hank coordinator configuration", flowProcess);
    Configurator configurator;
    try {
      configurator = (Configurator) new ObjectInputStream(new ByteArrayInputStream(
          Base64.decodeBase64(configuratorString.getBytes()))).readObject();
    } catch (Exception e) {
      throw new RuntimeException("Hank Configurator is incorrectly serialized in configuration item: " + configurationItem, e);
    }
    return configurator;
  }

  private static String getRequiredConfigurationItem(String key, String prettyName, FlowProcess flowProcess) {
    String result = (String) flowProcess.getProperty(key);
    if (result == null) {
      throw new RuntimeException(prettyName + " must be set with configuration item: " + key);
    }
    return result;
  }

  // JobConf
  public static Domain getDomain(JobConf conf) {
    String domainName = getDomainName(conf);
    return getConfigurator(conf).getCoordinator().getDomain(domainName);
  }

  public static String getDomainName(JobConf conf) {
    return getRequiredConfigurationItem(DomainBuilderOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME,
        "Hank domain name", conf);
  }

  public static Configurator getConfigurator(JobConf conf) {
    String domainName = getDomainName(conf);
    String configurationItem = DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_CONFIGURATOR);
    String configuratorString = getRequiredConfigurationItem(configurationItem,
        "Hank coordinator configuration", conf);
    Configurator configurator;
    try {
      configurator = (Configurator) new ObjectInputStream(new ByteArrayInputStream(
          Base64.decodeBase64(configuratorString.getBytes()))).readObject();
    } catch (Exception e) {
      throw new RuntimeException("Hank Configurator is incorrectly serialized in configuration item: " + configurationItem, e);
    }
    return configurator;
  }

  public static VersionType getVersionType(String domainName, JobConf conf) {
    return VersionType.valueOf(getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_TYPE),
        "Hank version type (base or delta)", conf));
  }

  public static String getOutputPath(String domainName, JobConf conf) {
    return getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH),
        "Hank output path", conf);
  }

  public static String getTmpOutputPath(String domainName, JobConf conf) {
    return getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_TMP_OUTPUT_PATH),
        "Hank temporary output path", conf);
  }

  public static Integer getVersionNumber(String domainName, JobConf conf) {
    return Integer.valueOf(getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        DomainBuilderOutputFormat.CONF_PARAM_HANK_VERSION_NUMBER),
        "Hank temporary output path", conf));
  }

  public static String getRequiredConfigurationItem(String key, String prettyName, JobConf conf) {
    String result = conf.get(key);
    if (result == null) {
      throw new RuntimeException(prettyName + " must be set with configuration item: " + key);
    }
    return result;
  }

  static public String getRemoteDomainRoot(String domainName, Configurator configurator) {
    Domain domain = configurator.getCoordinator().getDomain(domainName);
    if (domain == null) {
      throw new RuntimeException("Could not get domain: " + domainName + " from coordinator.");
    }
    Map<String, Object> options = domain.getStorageEngineOptions();
    if (options == null) {
      throw new RuntimeException("Empty options for domain: " + domainName);
    }
    String result = (String) options.get(REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION);
    if (result == null) {
      throw new RuntimeException("Could not load option: " + REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION + " for domain: " + domainName + " from storage engine options.");
    }
    return result;
  }

  // Builds a base64 encoded string of the serialized configurator
  private static String buildConfigurationString(Configurator configurator) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      new ObjectOutputStream(baos).writeObject(configurator);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new String(Base64.encodeBase64(baos.toByteArray()));
  }
}
