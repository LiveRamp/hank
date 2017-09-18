package com.liveramp.hank.fixtures;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlClientConfigurator;
import com.liveramp.hank.config.yaml.YamlPartitionServerConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.PartitionServerAddress;

public class ConfigFixtures {
  public static String coordinatorConfig(int zkPort,
                                         String domainsRoot,
                                         String domainGroupsRoot,
                                         String ringGroupsRoot) {

    StringBuilder builder = new StringBuilder();

    builder.append(
        "coordinator:\n" +
            "  factory: com.liveramp.hank.coordinator.zk.ZooKeeperCoordinator$Factory\n" +
            "  options:\n" +
            "    connect_string: localhost:").append(zkPort).append("\n")
        .append("    session_timeout: 1000000\n");
    builder.append("    domains_root: ").append(domainsRoot).append("\n");
    builder.append("    domain_groups_root: ").append(domainGroupsRoot).append("\n");
    builder.append("    ring_groups_root: ").append(ringGroupsRoot).append("\n");
    builder.append("    max_connection_attempts: 5\n");

    return builder.toString();
  }

  public static String partitionServerConfig(String dataDir,
                                             String ringGroupName,
                                             int zkPort,
                                             PartitionServerAddress addy,
                                             String domainsRoot,
                                             String domainGroupsRoot,
                                             String ringGroupsRoot){

    StringBuilder builder = new StringBuilder();

    builder.append(YamlPartitionServerConfigurator.PARTITION_SERVER_SECTION_KEY + ":\n");
    builder.append("  " + YamlPartitionServerConfigurator.SERVICE_PORT_KEY + ": " + addy.getPortNumber()+"\n");
    builder.append("  " + YamlPartitionServerConfigurator.RING_GROUP_NAME_KEY + ": "+ringGroupName+"\n");
    builder.append("  " + YamlPartitionServerConfigurator.LOCAL_DATA_DIRS_KEY + ":\n");
    builder.append("    - " + dataDir+"\n");
    builder.append("  " + YamlPartitionServerConfigurator.PARTITION_SERVER_DAEMON_SECTION_KEY + ":\n");
    builder.append("    " + YamlPartitionServerConfigurator.NUM_CONCURRENT_QUERIES_KEY + ": 1\n");
    builder.append("    " + YamlPartitionServerConfigurator.NUM_CONCURRENT_GET_BULK_TASKS + ": 1\n");
    builder.append("    " + YamlPartitionServerConfigurator.GET_BULK_TASK_SIZE + ": 2\n");
    builder.append("    " + YamlPartitionServerConfigurator.GET_TIMER_AGGREGATOR_WINDOW_KEY + ": 1000\n");
    builder.append("    " + YamlPartitionServerConfigurator.CACHE_NUM_BYTES_CAPACITY + ": 1000000\n");
    builder.append("    " + YamlPartitionServerConfigurator.CACHE_NUM_ITEMS_CAPACITY + ": 1000000\n");
    builder.append("    " + YamlPartitionServerConfigurator.BUFFER_REUSE_MAX_SIZE + ": 0\n");
    builder.append("  " + YamlPartitionServerConfigurator.UPDATE_DAEMON_SECTION_KEY + ":\n");
    builder.append("    " + YamlPartitionServerConfigurator.NUM_CONCURRENT_UPDATES_KEY + ": 1\n");
    builder.append("    " + YamlPartitionServerConfigurator.MAX_CONCURRENT_UPDATES_PER_DATA_DIRECTORY_KEY + ": 1\n");
    builder.append(coordinatorConfig(zkPort, domainsRoot, domainGroupsRoot, ringGroupsRoot));

    return builder.toString();
  }

  public static Coordinator createCoordinator(String tmpDir,
                                              int zkPort,
                                              String domainsRoot,
                                              String domainGroupsRoot,
                                              String ringGroupsRoot) throws IOException, InvalidConfigurationException {

    String tmpFile = tmpDir + "/" + UUID.randomUUID().toString();

    FileWriter fileWriter = new FileWriter(tmpFile);
    fileWriter.append(coordinatorConfig(zkPort, domainsRoot, domainGroupsRoot, ringGroupsRoot));
    fileWriter.close();

    CoordinatorConfigurator config = new YamlClientConfigurator(tmpFile);

    return config.createCoordinator();

  }

}
