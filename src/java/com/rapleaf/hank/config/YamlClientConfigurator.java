package com.rapleaf.hank.config;

import java.io.IOException;

public class YamlClientConfigurator extends BaseYamlConfigurator implements ClientConfigurator {
  public YamlClientConfigurator(String path) throws IOException,
      InvalidConfigurationException {
    super(path);
  }
}
