package com.rapleaf.hank.config;

import java.io.IOException;
import java.util.Map;

public class YamlSmartClientDaemonConfigurator extends BaseYamlConfigurator implements SmartClientDaemonConfigurator {

  private static final String SMART_CLIENT_SECTION_KEY = "smart_client";
  private static final String NUM_WORKER_THREADS = "num_worker_threads";
  private static final String SERVICE_PORT_KEY = "service_port";
  private static final String RING_GROUP_NAME_KEY = "ring_group_name";

  public YamlSmartClientDaemonConfigurator(String path) throws IOException, InvalidConfigurationException {
    super(path);
  }

  @Override
  public int getNumThreads() {
    return ((Integer)((Map<String, Object>) config.get(SMART_CLIENT_SECTION_KEY)).get(NUM_WORKER_THREADS)).intValue();
  }

  @Override
  public int getPortNumber() {
    return ((Integer)((Map<String, Object>) config.get(SMART_CLIENT_SECTION_KEY)).get(SERVICE_PORT_KEY)).intValue();
  }

  @Override
  public String getRingGroupName() {
    return (String) ((Map<String, Object>) config.get(SMART_CLIENT_SECTION_KEY)).get(RING_GROUP_NAME_KEY);
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();
    if (!config.containsKey(SMART_CLIENT_SECTION_KEY)) {
      throw new InvalidConfigurationException("Configuration must contain a 'smart_client' section!");
    }
    Map<String, Object> smartClientSection = (Map<String, Object>) config.get(SMART_CLIENT_SECTION_KEY);
    if (smartClientSection == null) {
      throw new InvalidConfigurationException("'smart_client' section may not be empty!");
    }
    if (!smartClientSection.containsKey(NUM_WORKER_THREADS) || !(smartClientSection.get(NUM_WORKER_THREADS) instanceof Integer)) {
      throw new InvalidConfigurationException("'smart_client' section must contain a 'num_worker_threads' key of type int!");
    }
    if (!smartClientSection.containsKey(SERVICE_PORT_KEY) || !(smartClientSection.get(SERVICE_PORT_KEY) instanceof Integer)) {
      throw new InvalidConfigurationException("'smart_client' section must contain a 'service_port' key of type int!");
    }
    if (!smartClientSection.containsKey(RING_GROUP_NAME_KEY)) {
      throw new InvalidConfigurationException("'smart_client' section must contain a 'ring_group_name' key!");
    }
  }

}
