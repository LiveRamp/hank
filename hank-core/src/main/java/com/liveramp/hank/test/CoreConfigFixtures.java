package com.liveramp.hank.test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlCoordinatorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;

public class CoreConfigFixtures {

  public static String coordinatorConfig(int zkPort,
                                         int sessionTimeout,
                                         String domainsRoot,
                                         String domainGroupsRoot,
                                         String ringGroupsRoot) {

    StringBuilder builder = new StringBuilder();

    builder.append(
        "coordinator:\n" +
            "  factory: com.liveramp.hank.coordinator.zk.ZooKeeperCoordinator$Factory\n" +
            "  options:\n" +
            "    connect_string: localhost:").append(zkPort).append("\n")
        .append("    session_timeout: "+sessionTimeout+"\n");
    builder.append("    domains_root: ").append(domainsRoot).append("\n");
    builder.append("    domain_groups_root: ").append(domainGroupsRoot).append("\n");
    builder.append("    ring_groups_root: ").append(ringGroupsRoot).append("\n");
    builder.append("    max_connection_attempts: 5\n");

    return builder.toString();
  }

  public static Coordinator createCoordinator(String tmpDir,
                                              int zkPort,
                                              int sessionTimeout,
                                              String domainsRoot,
                                              String domainGroupsRoot,
                                              String ringGroupsRoot) throws IOException, InvalidConfigurationException {

    String tmpFile = tmpDir + "/" + UUID.randomUUID().toString();

    FileWriter fileWriter = new FileWriter(tmpFile);
    fileWriter.append(coordinatorConfig(zkPort, sessionTimeout, domainsRoot, domainGroupsRoot, ringGroupsRoot));
    fileWriter.close();

    CoordinatorConfigurator config = new YamlCoordinatorConfigurator(tmpFile);

    return config.createCoordinator();

  }


}
