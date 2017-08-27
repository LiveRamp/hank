package com.liveramp.hank.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;

import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.DomainVersionProperties;
import com.liveramp.hank.coordinator.RunWithCoordinator;
import com.liveramp.hank.coordinator.RunnableWithCoordinator;
import com.liveramp.hank.storage.FileOpsUtil;

public class DomainBuilderProperties {

  private static final String TMP_OUTPUT_PATH = "_temporary/";

  public static final String REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION = "remote_domain_root";

  private static final Class<? extends DomainBuilderAbstractOutputFormat> DEFAULT_OUTPUT_FORMAT_CLASS
      = DomainBuilderDefaultOutputFormat.class;

  private final String domainName;
  private final CoordinatorConfigurator configurator;
  private Class<? extends DomainBuilderAbstractOutputFormat> outputFormatClass = DEFAULT_OUTPUT_FORMAT_CLASS;
  private String outputPath = null;
  private String randomTmpOutputPathId;
  private boolean shouldPartitionAndSortInput = true;
  private boolean shouldCloseVersion = true;

  private static final Logger LOG = LoggerFactory.getLogger(DomainBuilderProperties.class);

  // With a default output format
  // Get output path from the Coordinator
  public DomainBuilderProperties(String domainName,
                                 CoordinatorConfigurator configurator) {
    this.domainName = domainName;
    this.configurator = configurator;
  }

  // With a specific output format
  // Get output path from the Coordinator
  public DomainBuilderProperties(String domainName,
                                 CoordinatorConfigurator configurator,
                                 Class<? extends DomainBuilderAbstractOutputFormat> outputFormatClass) {
    this.domainName = domainName;
    this.configurator = configurator;
    this.outputFormatClass = outputFormatClass;
  }

  public String getDomainName() {
    return domainName;
  }

  public CoordinatorConfigurator getConfigurator() {
    return configurator;
  }

  public String getOutputPath() throws IOException {
    if (outputPath == null) {
      this.outputPath = getRemoteDomainRoot();
    }
    return outputPath;
  }

  public DomainBuilderProperties setOutputPath(String outputPath) {
    this.outputPath = outputPath;
    return this;
  }

  public String getTmpOutputPath(int versionNumber) {
    if (randomTmpOutputPathId == null) {
      randomTmpOutputPathId = UUID.randomUUID().toString();
    }
    return outputPath + "/" + TMP_OUTPUT_PATH + "version_" + versionNumber + "_" + randomTmpOutputPathId + "/";
  }

  public Class<? extends DomainBuilderAbstractOutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  public boolean shouldPartitionAndSortInput() {
    return shouldPartitionAndSortInput;
  }

  public boolean shouldCloseVersion(){
    return this.shouldCloseVersion;
  }

  public DomainBuilderProperties setShouldPartitionAndSortInput(boolean shouldPartitionAndSortInput) {
    this.shouldPartitionAndSortInput = shouldPartitionAndSortInput;
    return this;
  }

  public DomainBuilderProperties setShouldCloseVersion(boolean closeVersion){
    this.shouldCloseVersion = closeVersion;
    return this;
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

    //  this fixes hank domain building on YARN
    properties.setProperty("mapreduce.fileoutputcommitter.algorithm.version", "2");

    return properties;
  }

  // To configure Hadoop MapReduce jobs
  public JobConf setJobConfProperties(JobConf conf, int versionNumber) throws IOException {
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

    //  this fixes hank domain building on YARN
    conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2");

    return conf;
  }


  // TODO: maybe refactor and move the flow process stuff to the cascading package

  // FlowProcess

  private static String getRequiredConfigurationItem(String key, String prettyName, FlowProcess flowProcess) {
    String result = (String)flowProcess.getProperty(key);
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
      configurator = (CoordinatorConfigurator)new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(configuratorString.getBytes()))).readObject();
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
      configurator = (CoordinatorConfigurator)new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(configuratorString.getBytes()))).readObject();
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

  private static class DomainBuilderRemoteDomainRootGetter implements RunnableWithCoordinator {

    private final String domainName;
    private String result;

    public DomainBuilderRemoteDomainRootGetter(String domainName) {
      this.domainName = domainName;
    }

    @Override
    public void run(Coordinator coordinator) throws IOException {
      Domain domain = coordinator.getDomain(domainName);
      Map<String, Object> options = getOptions(domainName, domain);

      this.result = FileOpsUtil.getDomainBuilderRoot(options);
    }
  }

  private static class PartitionServerRemoteDomainRootGetter implements RunnableWithCoordinator {

    private final String domainName;
    private String result;

    public PartitionServerRemoteDomainRootGetter(String domainName) {
      this.domainName = domainName;
    }

    @Override
    public void run(Coordinator coordinator) throws IOException {
      Domain domain = coordinator.getDomain(domainName);
      Map<String, Object> options = getOptions(domainName, domain);

      this.result = FileOpsUtil.getPartitionServerRoot(options);
    }

  }

  private static Map<String, Object> getOptions(String domainName, Domain domain) {
    if (domain == null) {
      throw new RuntimeException("Could not get domain: " + domainName + " from coordinator.");
    }
    Map<String, Object> options = domain.getStorageEngineOptions();
    if (options == null) {
      throw new RuntimeException("Empty options for domain: " + domainName);
    }
    return options;
  }


  public String getRemoteDomainRoot() throws IOException {
    DomainBuilderRemoteDomainRootGetter remoteDomainRootGetter = new DomainBuilderRemoteDomainRootGetter(domainName);
    RunWithCoordinator.run(configurator, remoteDomainRootGetter);
    return remoteDomainRootGetter.result;
  }

  @Deprecated
  public static String getRemoteDomainRoot(Coordinator coordinator, String domainName) throws IOException {
    return getDomainBuilderDomainRoot(coordinator, domainName);
  }

  public static String getDomainBuilderDomainRoot(Coordinator coordinator, String domainName) throws IOException {
    if (coordinator == null) {
      throw new RuntimeException("A null Coordinator was provided.");
    }
    DomainBuilderRemoteDomainRootGetter remoteDomainRootGetter = new DomainBuilderRemoteDomainRootGetter(domainName);
    remoteDomainRootGetter.run(coordinator);
    return remoteDomainRootGetter.result;
  }

  public static String getPartitionServerDomainRoot(Coordinator coordinator, String domainName) throws IOException {
    if(coordinator == null){
      throw new RuntimeException("A null Coordinator was provided.");
    }
    PartitionServerRemoteDomainRootGetter remoteDomainRootGetter = new PartitionServerRemoteDomainRootGetter(domainName);
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
