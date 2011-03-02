package com.rapleaf.hank.config;

import java.io.IOException;
import java.util.Map;

public class YamlPartDaemonConfigurator extends BaseYamlPartservConfigurator
    implements PartDaemonConfigurator {
  private static final String PART_DAEMON_SECTION_KEY = "part_daemon";
  private static final String NUM_WORKER_THREADS = "num_worker_threads";

  public YamlPartDaemonConfigurator(String path) throws IOException,
      InvalidConfigurationException {
    super(path);
  }

  @Override
  public int getNumThreads() {
    return ((Integer)((Map<String, Object>) getPartservSection().get(PART_DAEMON_SECTION_KEY)).get(NUM_WORKER_THREADS)).intValue();
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();
    Map<String, Object> partservSection = getPartservSection();
    if(!partservSection.containsKey(PART_DAEMON_SECTION_KEY)) {
      throw new InvalidConfigurationException("'partserv' section must contain a 'part_daemon' key!");
    }
    Map<String, Object> partDaemonSection = (Map<String, Object>) partservSection.get(PART_DAEMON_SECTION_KEY);
    if (partDaemonSection == null) {
      throw new InvalidConfigurationException("'part_daemon' section must not be null!");
    }
    if (!partDaemonSection.containsKey(NUM_WORKER_THREADS) || !(partDaemonSection.get(NUM_WORKER_THREADS) instanceof Integer)) {
      throw new InvalidConfigurationException("'part_daemon' section must contain a 'num_worker_threads' key of type int!");
    }
  }
}
