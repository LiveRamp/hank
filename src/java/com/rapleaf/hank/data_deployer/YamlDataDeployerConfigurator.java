package com.rapleaf.hank.data_deployer;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.config.DataDeployerConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;

public class YamlDataDeployerConfigurator implements DataDeployerConfigurator {

  private static final String SLEEP_INTERVAL_KEY = "sleep_interval";
  private static final String RING_GROUP_NAME_KEY = "ring_group_name";
  private static final String COORDINATOR_KEY = "coordinator";
  private static final Object FACTORY_KEY = "factory";
  private static final Object COORDINATOR_OPTS_KEY = "options";
  private final int sleepInterval;
  private final String ringGroupName;
  private final Coordinator coordinator;

  public YamlDataDeployerConfigurator(String configPath) throws IOException {
    Map<String, Object> rawConfig = (Map<String, Object>) new Yaml().load(new FileReader(configPath));
    this.sleepInterval = (Integer) rawConfig.get(SLEEP_INTERVAL_KEY);
    this.ringGroupName = (String) rawConfig.get(RING_GROUP_NAME_KEY);
    Map<String, Object> coordinatorMap = (Map<String, Object>) rawConfig.get(COORDINATOR_KEY);
    String coordinatorFactoryClassName = (String) coordinatorMap.get(FACTORY_KEY);
    Map<String, Object> coordOpts = (Map<String, Object>) coordinatorMap.get(COORDINATOR_OPTS_KEY);
    this.coordinator = instantiate(coordinatorFactoryClassName, coordOpts);
  }

  private Coordinator instantiate(String coordinatorFactoryClassName, Map<String, Object> coordOpts) {
    try {
      CoordinatorFactory factory = (CoordinatorFactory) Class.forName(coordinatorFactoryClassName).newInstance();
      return factory.getCoordinator(coordOpts);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate coordinator factory "
          + coordinatorFactoryClassName + "!", e);
    } 
  }

  @Override
  public Coordinator getCoordinator() {
    return coordinator;
  }

  @Override
  public String getRingGroupName() {
    return ringGroupName;
  }

  @Override
  public long getSleepInterval() {
    return sleepInterval;
  }
}
