package com.rapleaf.hank.config;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaseYamlPartservConfigurator extends BaseYamlConfigurator implements PartservConfigurator {
  private static final String PARTSERV_SECTION_KEY = "partserv";
  private static final String LOCAL_DATA_DIRS_KEY = "local_data_dirs";
  private static final String SERVICE_PORT_KEY = "service_port";
  private static final String RING_GROUP_NAME_KEY = "ring_group_name";

  protected BaseYamlPartservConfigurator(String path) throws IOException,
  InvalidConfigurationException {
    super(path);
  }

  @Override
  public Set<String> getLocalDataDirectories() {
    return new HashSet<String>((Collection<? extends String>) getPartservSection().get(LOCAL_DATA_DIRS_KEY));
  }

  protected Map<String, Object> getPartservSection() {
    return (Map<String, Object>) config.get(PARTSERV_SECTION_KEY);
  }

  @Override
  public String getRingGroupName() {
    return (String) getPartservSection().get(RING_GROUP_NAME_KEY);
  }

  @Override
  public int getServicePort() {
    return ((Integer)getPartservSection().get(SERVICE_PORT_KEY)).intValue();
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();
    if (config.containsKey(PARTSERV_SECTION_KEY)) {
      Map<String, Object> partservSection = (Map<String, Object>) config.get(PARTSERV_SECTION_KEY);
      if (partservSection == null) {
        throw new InvalidConfigurationException("'partserv' section must not be null!");
      }
      if (!partservSection.containsKey(LOCAL_DATA_DIRS_KEY) || !(partservSection.get(LOCAL_DATA_DIRS_KEY) instanceof List)) {
        throw new InvalidConfigurationException("'partserv' section must contain a 'local_data_dirs' key of type List!");
      }
      if (!partservSection.containsKey(SERVICE_PORT_KEY) || !(partservSection.get(SERVICE_PORT_KEY) instanceof Integer)) {
        throw new InvalidConfigurationException("'partserv' section must contain a 'service_port' key of type int!");
      }
      if (!partservSection.containsKey(RING_GROUP_NAME_KEY)) {
        throw new InvalidConfigurationException("'partserv' section must contain a 'ring_group_name' key!");
      }
    } else {
      throw new InvalidConfigurationException("Configuration must contain a 'partserv' section!");
    }
  }
}
