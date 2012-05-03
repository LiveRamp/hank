package com.rapleaf.hank.hadoop;

import cascading.flow.FlowProcess;
import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.coordinator.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class DomainBuilderProperties {

  private static final String TMP_OUTPUT_PATH = "_temporary/";

  public static final String REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION = "remote_domain_root";

  private static final Class<? extends DomainBuilderAbstractOutputFormat> DEFAULT_OUTPUT_FORMAT_CLASS
      = DomainBuilderDefaultOutputFormat.class;

  private final String domainName;
  private final CoordinatorConfigurator configurator;
  private final String outputPath;
  private final Class<? extends DomainBuilderAbstractOutputFormat> outputFormatClass;
  private String randomTmpOutputPathId;
  private static final Logger LOG = Logger.getLogger(DomainBuilderProperties.class);

  // With a default output format
  // Get output path from the Coordinator
  public DomainBuilderProperties(String domainName,
                                 CoordinatorConfigurator configurator) throws IOException {
    this.domainName = domainName;
    this.configurator = configurator;
    this.outputPath = getRemoteDomainRoot();
    this.outputFormatClass = DEFAULT_OUTPUT_FORMAT_CLASS;
  }

  // With a specific output format
  // Get output path from the Coordinator
  public DomainBuilderProperties(String domainName,
                                 CoordinatorConfigurator configurator,
                                 Class<? extends DomainBuilderAbstractOutputFormat> outputFormatClass) throws IOException {
    this.domainName = domainName;
    this.configurator = configurator;
    this.outputPath = getRemoteDomainRoot();
    this.outputFormatClass = outputFormatClass;
  }

  // With a default output format
  // With a specific output path
  public DomainBuilderProperties(String domainName,
                                 CoordinatorConfigurator configurator,
                                 String outputPath) {
    this.domainName = domainName;
    this.configurator = configurator;
    this.outputPath = outputPath;
    this.outputFormatClass = DEFAULT_OUTPUT_FORMAT_CLASS;
  }

  // With a specific output format
  // With a specific output path
  public DomainBuilderProperties(String domainName,
                                 CoordinatorConfigurator configurator,
                                 String outputPath,
                                 Class<? extends DomainBuilderAbstractOutputFormat> outputFormatClass) {
    this.domainName = domainName;
    this.configurator = configurator;
    this.outputPath = outputPath;
    this.outputFormatClass = outputFormatClass;
  }

  public String getDomainName() {
    return domainName;
  }

  public CoordinatorConfigurator getConfigurator() {
    return configurator;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public String getTmpOutputPath(int versionNumber) {
    if (randomTmpOutputPathId == null) {
      randomTmpOutputPathId = UUID.randomUUID().toString();
    }
    return outputPath + "/" + TMP_OUTPUT_PATH + "version-" + versionNumber + "_" + randomTmpOutputPathId + "/";
  }

  public Class<? extends DomainBuilderAbstractOutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  // To configure cascading jobs
  public Properties setCascadingProperties(Properties properties,
                                           int versionNumber,
                                           int numPartitions) {

    // Note: Domain name is set locally in DomainBuilderTap to deal with Cascading
    // jobs building multiple domains.

    // Configuration
    properties.setProperty(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_CONFIGURATOR),
        buildConfigurationString(configurator));
    // Output Path
    properties.setProperty(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH), outputPath);
    // Tmp output path
    properties.setProperty(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_TMP_OUTPUT_PATH),
        getTmpOutputPath(versionNumber));
    // Version Number
    properties.setProperty(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_VERSION_NUMBER),
        Integer.toString(versionNumber));
    // Number of partitions
    properties.setProperty(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_NUM_PARTITIONS),
        Integer.toString(numPartitions));
    // Number of reduce tasks is set to the maximum number of partitions to build for a single domain
    // When moving to Cascading 2.0 we will be able to set the number of reduce tasks per step (for each domain)
    Integer numPartitionsPrevious = 0;
    String numPartitionsStr = properties.getProperty("mapred.reduce.tasks");
    if (numPartitionsStr != null) {
      numPartitionsPrevious = Integer.valueOf(numPartitionsStr);
    }
    if (numPartitions > numPartitionsPrevious) {
      properties.setProperty("mapred.reduce.tasks", String.valueOf(numPartitions));
    }
    return properties;
  }

  // To configure Hadoop MapReduce jobs
  public JobConf setJobConfProperties(JobConf conf, int versionNumber) {
    // Domain name
    conf.set(DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME, getDomainName());
    // Configuration
    conf.set(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_CONFIGURATOR),
        buildConfigurationString(configurator));
    // Output path
    conf.set(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH),
        getOutputPath());
    // Tmp output path
    conf.set(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_TMP_OUTPUT_PATH),
        getTmpOutputPath(versionNumber));
    // Version Number
    conf.set(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_VERSION_NUMBER),
        Integer.toString(versionNumber));
    return conf;
  }

  // TODO: maybe refactor and move the flow process stuff to the cascading package

  // FlowProcess

  private static String getRequiredConfigurationItem(String key, String prettyName, FlowProcess flowProcess) {
    String result = (String) flowProcess.getProperty(key);
    if (result == null) {
      throw new RuntimeException(prettyName + " must be set with configuration item: " + key);
    }
    return result;
  }

  // JobConf

  public static String getDomainName(JobConf conf) {
    return getRequiredConfigurationItem(DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME,
        "Hank domain name", conf);
  }

  public static CoordinatorConfigurator getConfigurator(JobConf conf) {
    String domainName = getDomainName(conf);
    String configurationItem = DomainBuilderAbstractOutputFormat.createConfParamName(domainName,
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_CONFIGURATOR);
    String configuratorString = getRequiredConfigurationItem(configurationItem,
        "Hank coordinator configuration", conf);
    CoordinatorConfigurator configurator;
    try {
      configurator = (CoordinatorConfigurator) new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(configuratorString.getBytes()))).readObject();
    } catch (Exception e) {
      throw new RuntimeException("Hank Configurator is incorrectly serialized in configuration item: " + configurationItem, e);
    }
    return configurator;
  }

  public static CoordinatorConfigurator getConfigurator(String domainName, FlowProcess flowProcess) {
    String configurationItem = DomainBuilderAbstractOutputFormat.createConfParamName(domainName,
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_CONFIGURATOR);
    String configuratorString = getRequiredConfigurationItem(configurationItem,
        "Hank coordinator configuration", flowProcess);
    CoordinatorConfigurator configurator;
    try {
      configurator = (CoordinatorConfigurator) new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(configuratorString.getBytes()))).readObject();
    } catch (Exception e) {
      throw new RuntimeException("Hank Configurator is incorrectly serialized in configuration item: " + configurationItem, e);
    }
    return configurator;
  }

  public static String getOutputPath(String domainName, JobConf conf) {
    return getRequiredConfigurationItem(DomainBuilderAbstractOutputFormat.createConfParamName(domainName,
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH),
        "Hank output path", conf);
  }

  public static String getTmpOutputPath(String domainName, JobConf conf) {
    return getRequiredConfigurationItem(DomainBuilderAbstractOutputFormat.createConfParamName(domainName,
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_TMP_OUTPUT_PATH),
        "Hank temporary output path", conf);
  }

  public static Integer getVersionNumber(String domainName, JobConf conf) {
    return Integer.valueOf(getRequiredConfigurationItem(DomainBuilderAbstractOutputFormat.createConfParamName(domainName,
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_VERSION_NUMBER),
        "Hank version number", conf));
  }

  public static Integer getNumPartitions(String domainName, JobConf conf) {
    return Integer.valueOf(getRequiredConfigurationItem(DomainBuilderAbstractOutputFormat.createConfParamName(domainName,
        DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_NUM_PARTITIONS),
        "Hank number of partitions", conf));
  }

  public static String getRequiredConfigurationItem(String key, String prettyName, JobConf conf) {
    String result = conf.get(key);
    if (result == null) {
      throw new RuntimeException(prettyName + " must be set with configuration item: " + key);
    }
    return result;
  }

  private static class RemoteDomainRootGetter implements RunnableWithCoordinator {

    private final String domainName;
    private String result;

    public RemoteDomainRootGetter(String domainName) {
      this.domainName = domainName;
    }

    @Override
    public void run(Coordinator coordinator) throws IOException {
      Domain domain = coordinator.getDomain(domainName);
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
      this.result = result;
    }
  }

  public String getRemoteDomainRoot() throws IOException {
    RemoteDomainRootGetter remoteDomainRootGetter = new RemoteDomainRootGetter(domainName);
    RunWithCoordinator.run(configurator, remoteDomainRootGetter);
    return remoteDomainRootGetter.result;
  }

  public static String getRemoteDomainRoot(Coordinator coordinator, String domainName) throws IOException {
    if (coordinator == null) {
      throw new RuntimeException("A null Coordinator was provided.");
    }
    RemoteDomainRootGetter remoteDomainRootGetter = new RemoteDomainRootGetter(domainName);
    remoteDomainRootGetter.run(coordinator);
    return remoteDomainRootGetter.result;
  }

  public DomainVersionNumberAndNumPartitions openVersion(DomainVersionProperties domainVersionProperties) throws IOException {
    return openVersion(getConfigurator(), getDomainName(), domainVersionProperties);
  }

  public void cancelVersion(Integer domainVersionNumber) throws IOException {
    cancelVersion(getConfigurator(), getDomainName(), domainVersionNumber);
  }

  public void closeVersion(Integer domainVersionNumber) throws IOException {
    closeVersion(getConfigurator(), getDomainName(), domainVersionNumber);
  }

  public static DomainVersionNumberAndNumPartitions openVersion(CoordinatorConfigurator configurator,
                                                                String domainName,
                                                                DomainVersionProperties domainVersionProperties) throws IOException {
    DomainVersionOpener domainVersionOpener = new DomainVersionOpener(domainName, domainVersionProperties);
    RunWithCoordinator.run(configurator, domainVersionOpener);
    return domainVersionOpener.result;
  }

  public static void cancelVersion(CoordinatorConfigurator configurator,
                                   String domainName,
                                   Integer domainVersionNumber) throws IOException {
    RunWithCoordinator.run(configurator, new DomainVersionCanceller(domainName, domainVersionNumber));
  }

  public static void closeVersion(CoordinatorConfigurator configurator,
                                  String domainName,
                                  Integer domainVersionNumber) throws IOException {
    RunWithCoordinator.run(configurator, new DomainVersionCloser(domainName, domainVersionNumber));
  }

  public static Domain getDomain(Coordinator coordinator, String domainName) throws IOException {
    Domain domain = coordinator.getDomainShallow(domainName);
    // Fail if unable to load domain
    if (domain == null) {
      throw new IOException("Could not load Domain: " + domainName + " with coordinator: " + coordinator);
    }
    return domain;
  }

  public static DomainVersion getDomainVersion(Coordinator coordinator, String domainName, Integer domainVersionNumber) throws IOException {
    if (domainVersionNumber == null) {
      return null;
    }
    Domain domain = getDomain(coordinator, domainName);
    DomainVersion domainVersion = domain.getVersionShallow(domainVersionNumber);
    if (domainVersion == null) {
      throw new IOException("Could not get version " + domainVersionNumber + " of domain " + domainName
          + " with coordinator: " + coordinator);
    } else {
      return domainVersion;
    }
  }

  // Builds a base64 encoded string of the serialized configurator
  private static String buildConfigurationString(CoordinatorConfigurator configurator) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      new ObjectOutputStream(baos).writeObject(configurator);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new String(Base64.encodeBase64(baos.toByteArray()));
  }

  private static class DomainVersionOpener implements RunnableWithCoordinator {

    private final String domainName;
    private final DomainVersionProperties domainVersionProperties;
    private DomainVersionNumberAndNumPartitions result;

    public DomainVersionOpener(String domainName,
                               DomainVersionProperties domainVersionProperties) {
      this.domainName = domainName;
      this.domainVersionProperties = domainVersionProperties;
    }

    @Override
    public void run(Coordinator coordinator) throws IOException {
      Domain domain = getDomain(coordinator, domainName);
      DomainVersion domainVersion = domain.openNewVersion(domainVersionProperties);
      if (domainVersion == null) {
        throw new IOException("Could not open a new version of domain " + domainName);
      } else {
        LOG.info("Opened new version #" + domainVersion.getVersionNumber() + " of domain: " + domainName);
        result = new DomainVersionNumberAndNumPartitions(domainVersion.getVersionNumber(), domain.getNumParts());
      }
    }
  }

  private static class DomainVersionCanceller implements RunnableWithCoordinator {

    private final String domainName;
    private final Integer domainVersionNumber;

    public DomainVersionCanceller(String domainName,
                                  Integer domainVersionNumber) {
      this.domainName = domainName;
      this.domainVersionNumber = domainVersionNumber;
    }

    @Override
    public void run(Coordinator coordinator) throws IOException {
      DomainVersion domainVersion = getDomainVersion(coordinator, domainName, domainVersionNumber);
      LOG.info("Cancelling new version #" + domainVersion.getVersionNumber() + " of domain: " + domainName);
      domainVersion.cancel();
    }
  }

  private static class DomainVersionCloser implements RunnableWithCoordinator {

    private final String domainName;
    private final Integer domainVersionNumber;

    public DomainVersionCloser(String domainName,
                               Integer domainVersionNumber) {
      this.domainName = domainName;
      this.domainVersionNumber = domainVersionNumber;
    }

    @Override
    public void run(Coordinator coordinator) throws IOException {
      DomainVersion domainVersion = getDomainVersion(coordinator, domainName, domainVersionNumber);
      LOG.info("Closing new version #" + domainVersion.getVersionNumber() + " of domain: " + domainName);
      domainVersion.close();
    }
  }

  public String toString() {
    return "<DomainBuilderProperties: domain name: " + domainName
        + ", configurator: " + configurator
        + ", output path: " + outputPath
        + ", output format class: " + outputFormatClass
        + ">";
  }
}
