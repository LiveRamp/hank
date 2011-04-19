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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;

public class ZkTestCase extends BaseTestCase {
  private static final Logger LOG = Logger.getLogger(ZkTestCase.class);

  private static final int TICK_TIME = 2000;
  private static final int CONNECTION_TIMEOUT = 30000;

  protected static final int WAIT_TIME = 10000;

  private final static String zkDir = System.getProperty("zk_dir", "/tmp/zk_in_tests");
  private static Factory standaloneServerFactory;
  private static ZooKeeperServer server;
  private static int zkClientPort;

  private final String zkRoot;
  private ZooKeeper zk;

  public ZkTestCase() {
    super();
    zkRoot = "/" + getClass().getSimpleName();
  }

  public static void setupZkServer() throws Exception {
    if (server == null) {
      LOG.debug("deleting zk data dir (" + zkDir +")");
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

    zk = new ZooKeeper("127.0.0.1:" + zkClientPort, 1000000, new Watcher() {
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

    deleteNodeRecursively(zkRoot);
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

  public ZooKeeper getZk() {
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
    String nodePath = parentPath + "/" + nodeName;
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

  // TODO: this is inefficient. tokenize on / and then just make all the nodes iteratively.
  protected void createNodeRecursively(String path)
  throws InterruptedException {
    try {
      zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException.NoNodeException e) {
      String parentPath = path.substring(0, path.lastIndexOf('/'));
      createNodeRecursively(parentPath);
      createNodeRecursively(path);
    } catch (KeeperException e) {
      LOG.warn(e);
    }
  }

  protected void deleteNodeRecursively(String path) throws Exception {
    if (zk.exists(path, null) == null) {
      return;
    }
    List<String> children = zk.getChildren(path, null);
    for (String child : children) {
      deleteNodeRecursively(path + "/" + child);
    }
    zk.delete(path, -1);
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
