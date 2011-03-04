package com.rapleaf.hank.data_deployer;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import junit.framework.TestCase;

import com.rapleaf.hank.coordinator.MockCoordinator;

public class TestYamlDataDeployerConfigurator extends TestCase {

  private static final String PATH = "/tmp/yaml_data_deployer_configurator.yml";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    new File(PATH).delete();
    PrintWriter pw = new PrintWriter(new FileWriter(PATH));
    pw.println("---");
    pw.println("data_deployer:");
    pw.println("  ring_group_name: myRingGroup");
    pw.println("  sleep_interval: 1000");
    pw.println("coordinator:");
    pw.println("  factory: com.rapleaf.hank.coordinator.MockCoordinator$Factory");
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
  }

  public void testIt() throws Exception {
    YamlDataDeployerConfigurator c = new YamlDataDeployerConfigurator(PATH);
    assertEquals(1000, c.getSleepInterval());
    assertEquals("myRingGroup", c.getRingGroupName());
    assertTrue(c.getCoordinator() instanceof MockCoordinator);
    assertTrue(((MockCoordinator)c.getCoordinator()).getInitOptions().containsKey("blah"));
  }
}
