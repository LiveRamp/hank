package com.rapleaf.hank.config.yaml;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.MockRingGroup;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.monitor.notifier.MockNotifier;
import com.rapleaf.hank.monitor.notifier.Notifier;

import java.io.FileWriter;
import java.io.PrintWriter;

public class TestYamlMonitorConfigurator extends BaseTestCase {

  private final String configPath = localTmpDir + "/config.yml";

  public void testIt() throws Exception {
    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.println("monitor:");
    pw.println("  global_notifier_factory: com.rapleaf.hank.monitor.notifier.MockNotifierFactory");
    pw.println("  global_notifier_configuration:");
    pw.println("    a: a");

    pw.println("  ring_group_notifiers:");
    pw.println("    rg1:");
    pw.println("      factory: com.rapleaf.hank.monitor.notifier.MockNotifierFactory");
    pw.println("      configuration:");
    pw.println("        b: b");

    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();

    YamlMonitorConfigurator configurator = new YamlMonitorConfigurator(configPath);

    Notifier globalNotifier = configurator.getGlobalNotifier();
    assertTrue(globalNotifier instanceof MockNotifier);
    assertEquals("a", ((MockNotifier) globalNotifier).getConfiguration().get("a"));

    Notifier ringNotifier = configurator.getRingGroupNotifier(new MockRingGroup(null, "rg1", null));
    assertTrue(ringNotifier instanceof MockNotifier);
    assertEquals("b", ((MockNotifier) ringNotifier).getConfiguration().get("b"));
  }

}
