/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.rapleaf.hank;

import com.rapleaf.hank.client.HankSmartClient;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator;
import com.rapleaf.hank.generated.ClientMetadata;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.storage.echo.Echo;
import com.rapleaf.hank.util.Condition;
import com.rapleaf.hank.util.LocalHostUtils;
import com.rapleaf.hank.util.WaitUntil;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.*;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZkTestCase extends BaseTestCase {
  private static final Logger LOG = Logger.getLogger(ZkTestCase.class);

  private static final int TICK_TIME = 2000;
  private static final int CONNECTION_TIMEOUT = 30000;

  private final static String zkDir = System.getProperty("zk_dir", "/tmp/zk_in_tests");
  private static Factory standaloneServerFactory;
  private static ZooKeeperServer server;
  private static int zkClientPort;

  private final String zkRoot;
  private ZooKeeperPlus zk;

  public ZkTestCase() {
    super();
    zkRoot = "/" + getClass().getSimpleName();
  }

  public static void setupZkServer() throws Exception {
    if (server == null) {
      LOG.debug("deleting zk data dir (" + zkDir + ")");
      File zkDirFile = new File(zkDir);
      FileUtils.deleteDirectory(zkDirFile);
      zkDirFile.mkdirs();

      server = new ZooKeeperServer(zkDirFile, zkDirFile, TICK_TIME);

      int clientPort = 2000;
      while (true) {
        LOG.debug("Trying to bind server to port " + clientPort);
        try {
          standaloneServerFactory =
              new NIOServerCnxn.Factory(new InetSocketAddress(clientPort));
        } catch (BindException e) {
          LOG.trace("Failed binding ZK Server to client port: " + clientPort);
          //this port is already in use. try to use another
          clientPort++;
          continue;
        }
        LOG.debug("Succeeded in binding ZK Server to client port " + clientPort);
        break;
      }
      standaloneServerFactory.startup(server);

      if (!waitForServerUp(clientPort, CONNECTION_TIMEOUT)) {
        throw new IOException("Waiting for startup of standalone server");
      }
      zkClientPort = clientPort;
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);

    setupZkServer();

    final Object lock = new Object();
    final AtomicBoolean connected = new AtomicBoolean(false);

    zk = new ZooKeeperPlus("127.0.0.1:" + zkClientPort, 1000000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getType()) {
          case None:
            if (event.getState() == KeeperState.SyncConnected) {
              connected.set(true);
              synchronized (lock) {
                lock.notifyAll();
              }
            }
        }
        LOG.debug(event.toString());
      }
    });

    synchronized (lock) {
      lock.wait(2000);
    }
    if (!connected.get()) {
      fail("timed out waiting for the zk client connection to come online!");
    }
    LOG.debug("session timeout: " + zk.getSessionTimeout());

    zk.deleteNodeRecursively(zkRoot);
    WaitUntil.condition(new Condition() {
      @Override
      public boolean test() {
        try {
          return zk.exists(zkRoot, false) == null;
        } catch (KeeperException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
    createNodeRecursively(zkRoot);
  }

  private static boolean waitForServerUp(int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        Socket sock = new Socket("localhost", port);
        BufferedReader reader = null;
        try {
          OutputStream outstream = sock.getOutputStream();
          outstream.write("stat".getBytes());
          outstream.flush();

          Reader isr = new InputStreamReader(sock.getInputStream());
          reader = new BufferedReader(isr);
          String line = reader.readLine();
          if (line != null && line.startsWith("Zookeeper version:")) {
            return true;
          }
        } finally {
          sock.close();
          if (reader != null) {
            reader.close();
          }
        }
      } catch (IOException e) {
        // ignore as this is expected
        LOG.info("server localhost:" + port + " not up " + e);
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }

  public String getRoot() {
    return zkRoot;
  }

  public ZooKeeperPlus getZk() {
    return zk;
  }

  @Override
  protected void tearDown() throws Exception {
    LOG.debug("teardown called");
    super.tearDown();
    shutdownZk();
  }

  public void shutdownZk() throws Exception {
    if (zk != null && zk.getState() == States.CONNECTED) {
      zk.close();
    }
    zk = null;
  }

  public int getZkClientPort() {
    return zkClientPort;
  }

  public String getZkConnectString() {
    return "localhost:" + zkClientPort;
  }

  public void dumpZk() throws Exception {
    dumpZk("", "", 0);
  }

  private void dumpZk(String parentPath, String nodeName, int depth) throws Exception {
    System.err.print(StringUtils.repeat("  ", depth));
    System.err.print("/" + nodeName);
    String nodePath = ZkPath.append(parentPath, nodeName);
    nodePath = nodePath.replace("//", "/");
    byte[] data = zk.getData(nodePath, false, null);
    if (data == null) {
      System.err.print(" -> null");
    } else {
      System.err.print(" -> [bytes]");
    }
    System.err.println();
    List<String> children = zk.getChildren(nodePath, false);
    for (String child : children) {
      dumpZk(nodePath, child, depth + 1);
    }
  }

  protected void createNodeRecursively(String path)
      throws InterruptedException, Exception {
    String[] toks = path.split("/");
    String newPath = "/";
    for (int i = 0; i < toks.length; i++) {
      newPath += toks[i];
      if (zk.exists(newPath, false) == null) {
        zk.create(newPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (i != 0) {
        newPath += "/";
      }
    }
  }

  protected void create(String path) throws Exception {
    create(path, (byte[]) null);
  }

  protected void create(String path, String data) throws Exception {
    create(path, data.getBytes());
  }

  protected void create(String path, byte[] data) throws Exception {
    getZk().create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  protected Coordinator getMockCoordinator() throws Exception {
    create(ZkPath.append(getRoot(), "domains"));
    create(ZkPath.append(getRoot(), "domain_groups"));
    create(ZkPath.append(getRoot(), "ring_groups"));

    final Coordinator coord = new ZooKeeperCoordinator.Factory().getCoordinator(
        ZooKeeperCoordinator.Factory.requiredOptions(getZkConnectString(), 100000000,
            ZkPath.append(getRoot(), "domains"),
            ZkPath.append(getRoot(), "domain_groups"),
            ZkPath.append(getRoot(), "ring_groups")));

    String d0Conf = "---\n  blah: blah\n  moreblah: blahblah";

    final Domain d0 = coord.addDomain("domain0", 32, Echo.Factory.class.getName(), d0Conf, Murmur64Partitioner.class.getName(), Collections.<String>emptyList());
    DomainVersion ver = d0.openNewVersion(null);
    ver.close();
    ver = d0.openNewVersion(null);
    final Domain d1 = coord.addDomain("domain1", 32, Echo.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), Collections.<String>emptyList());
    ver = d1.openNewVersion(null);
    dumpZk();
    ver.close();
    ver = d1.openNewVersion(null);
    ver.close();

    DomainGroup g1 = coord.addDomainGroup("Group_1");
    Map<Domain, Integer> g1Versions = new HashMap<Domain, Integer>();
    g1Versions.put(d0, 1);
    g1Versions.put(d1, 1);
    g1.setDomainVersions(g1Versions);

    DomainGroup g2 = coord.addDomainGroup("Group_2");
    Map<Domain, Integer> g2Versions = new HashMap<Domain, Integer>();
    g2Versions.put(d1, 1);
    g2.setDomainVersions(g2Versions);

    RingGroup rgAlpha = coord.addRingGroup("RG_Alpha", g1.getName());
    Ring r1 = rgAlpha.addRing(1);
    r1.addHost(addy("alpha-1-1"), Collections.<String>emptyList());
    r1.addHost(addy("alpha-1-2"), Collections.<String>emptyList());
    r1.addHost(addy("alpha-1-3"), Collections.<String>emptyList());
    Ring r2 = rgAlpha.addRing(2);
    r2.addHost(addy("alpha-2-1"), Collections.<String>emptyList());
    r2.addHost(addy("alpha-2-2"), Collections.<String>emptyList());
    r2.addHost(addy("alpha-2-3"), Collections.<String>emptyList());
    Ring r3 = rgAlpha.addRing(3);
    r3.addHost(addy("alpha-3-1"), Collections.<String>emptyList());
    r3.addHost(addy("alpha-3-2"), Collections.<String>emptyList());
    r3.addHost(addy("alpha-3-3"), Collections.<String>emptyList());
    for (int i = 0; i < 16; ++i) {
      rgAlpha.registerClient(new ClientMetadata(
          LocalHostUtils.getHostName(),
          System.currentTimeMillis(),
          HankSmartClient.class.getName(),
          Hank.getGitCommit()));
    }

    RingGroup rgBeta = coord.addRingGroup("RG_Beta", g1.getName());
    r1 = rgBeta.addRing(1);
    r1.addHost(addy("beta-1-1"), Collections.<String>emptyList());
    r1.addHost(addy("beta-1-2"), Collections.<String>emptyList());
    r1.addHost(addy("beta-1-3"), Collections.<String>emptyList());
    r1.addHost(addy("beta-1-4"), Collections.<String>emptyList());
    r2 = rgBeta.addRing(2);
    r2.addHost(addy("beta-2-1"), Collections.<String>emptyList());
    r2.addHost(addy("beta-2-2"), Collections.<String>emptyList());
    r2.addHost(addy("beta-2-3"), Collections.<String>emptyList());
    r2.addHost(addy("beta-2-4"), Collections.<String>emptyList());
    r3 = rgBeta.addRing(3);
    r3.addHost(addy("beta-3-1"), Collections.<String>emptyList());
    r3.addHost(addy("beta-3-2"), Collections.<String>emptyList());
    r3.addHost(addy("beta-3-3"), Collections.<String>emptyList());
    r3.addHost(addy("beta-3-4"), Collections.<String>emptyList());
    Ring r4 = rgBeta.addRing(4);
    r4.addHost(addy("beta-4-1"), Collections.<String>emptyList());
    r4.addHost(addy("beta-4-2"), Collections.<String>emptyList());
    r4.addHost(addy("beta-4-3"), Collections.<String>emptyList());
    r4.addHost(addy("beta-4-4"), Collections.<String>emptyList());

    RingGroup rgGamma = coord.addRingGroup("RG_Gamma", g2.getName());
    r1 = rgGamma.addRing(1);
    r1.addHost(addy("gamma-1-1"), Collections.<String>emptyList());

    return coord;
  }

  private PartitionServerAddress addy(String hostname) {
    return new PartitionServerAddress(hostname, 6200);
  }
}
