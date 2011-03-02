package com.rapleaf.hank.config;

import java.io.IOException;
import java.util.Map;

public class YamlUpdateDaemonConfigurator extends BaseYamlPartservConfigurator
    implements UpdateDaemonConfigurator {

  private static final String UPDATE_DAEMON_SECTION_KEY = "update_daemon";
  private static final String NUM_CONCURRENT_UPDATES_KEY = "num_concurrent_updates";

  public YamlUpdateDaemonConfigurator(String path) throws IOException, InvalidConfigurationException {
    super(path);
  }

  @Override
  public int getNumConcurrentUpdates() {
    return ((Integer)((Map<String, Object>) getPartservSection().get(UPDATE_DAEMON_SECTION_KEY)).get(NUM_CONCURRENT_UPDATES_KEY)).intValue();
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();
    Map<String, Object> partservSection = getPartservSection();
    if(!partservSection.containsKey(UPDATE_DAEMON_SECTION_KEY)) {
      throw new InvalidConfigurationException("'partserv' section must contain a 'update_daemon' key!");
    }
    Map<String, Object> updateDaemonSection = (Map<String, Object>) partservSection.get(UPDATE_DAEMON_SECTION_KEY);
    if (updateDaemonSection == null) {
      throw new InvalidConfigurationException("'update_daemon' section must not be null!");
    }
    if (!updateDaemonSection.containsKey(NUM_CONCURRENT_UPDATES_KEY) || !(updateDaemonSection.get(NUM_CONCURRENT_UPDATES_KEY) instanceof Integer)) {
      throw new InvalidConfigurationException("'update_daemon' section must contain a 'num_concurrent_updates' section of type int!");
    }
  }
}
