package com.liveramp.hank.ui;

import java.io.IOException;
import java.net.URL;
import java.util.Scanner;

import org.junit.Test;

import com.liveramp.commons.test.WaitUntil;
import com.liveramp.hank.config.MockMonitorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.zk.ZooKeeperCoordinator;
import com.liveramp.hank.test.CoreConfigFixtures;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.zookeeper.ZkPath;

public class TestWebUiServer extends ZkTestCase {

  private final String domainsRoot = ZkPath.append(getRoot(), "domains");
  private final String domainGroupsRoot = ZkPath.append(getRoot(), "domain_groups");
  private final String ringGroupsRoot = ZkPath.append(getRoot(), "ring_groups");

  @Test
  public void testShutdown() throws Exception {

    Coordinator coordinator = CoreConfigFixtures.createCoordinator(
        localTmpDir,
        getZkClientPort(),
        5000,
        domainsRoot,
        domainGroupsRoot,
        ringGroupsRoot
    );

    //  start web ui
    Thread webserverThread = new Thread(() -> {
      try {
        WebUiServer.start(
            new MockMonitorConfigurator(),
            coordinator,
            34724
        );
      } catch (Exception e) {
        System.out.println(e);
      }
    });

    webserverThread.start();

    //  verify that the ui is actually running
    WaitUntil.orDie(() -> hankUIRunning());

    //  this isn't perfect.  I'd rather kill and restart zookeeper.  the problem is that the local impl here
    //  doesn't persist state, so the session can't reconnect after zk is restarted.  hopefully this adequately
    //  captures the production issue of prolonged zk disconnects on restart.
    ZooKeeperCoordinator zkCoord = (ZooKeeperCoordinator) coordinator;
    expireSession(zkCoord.getSessionId());

    //  verify that the ui dies because the sessions expired
    WaitUntil.orDie(() -> !hankUIRunning());


  }

  private boolean hankUIRunning() {
    try {
      Scanner scanner = new Scanner(new URL("http://127.0.0.1:34724").openStream(), "UTF-8");
      String out = scanner.useDelimiter("\\A").next();
      scanner.close();
      return out.contains("System Summary");
    } catch (IOException e) {
      return false;
    }
  }


}