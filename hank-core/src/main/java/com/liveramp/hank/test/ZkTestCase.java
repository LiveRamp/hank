/**
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import com.liveramp.hank.zookeeper.ZkPath;
import com.liveramp.hank.zookeeper.ZooKeeperPlus;

import static org.junit.Assert.fail;

public abstract class ZkTestCase extends BaseTestCase {

  private static final Logger LOG = LoggerFactory.getLogger(ZkTestCase.class);

  private static final int TICK_TIME = 2000;
  private static final int CONNECTION_TIMEOUT = 30000;

  private final static String zkDir = System.getProperty("zk_dir", "/tmp/zk_in_tests");
  private static NIOServerCnxnFactory standaloneServerFactory;
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
              new NIOServerCnxnFactory();
          standaloneServerFactory.configure(new InetSocketAddress(clientPort), 100);
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

  @Before
  public final void setUpZk() throws Exception {

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
    zk.reconnect();

    synchronized (lock) {
      lock.wait(2000);
    }
    if (!connected.get()) {
      fail("timed out waiting for the zk client connection to come online!");
    }
    LOG.debug("session timeout: " + zk.getSessionTimeout());

    zk.deleteNodeRecursively(zkRoot);
    WaitUntil.orDie(new Condition() {
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

  @After
  public final void tearDownZkTestCase() throws Exception {
    LOG.debug("teardown called");
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
      throws Exception {
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
}
