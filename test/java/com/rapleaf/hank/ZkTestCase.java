package com.rapleaf.hank;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.BindException;
import java.net.Socket;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;

public class ZkTestCase extends BaseTestCase {
  private static final Logger LOG = Logger.getLogger(BaseTestCase.class);

  private static final int TICK_TIME = 2000;
  private static final int CONNECTION_TIMEOUT = 30000;

  private final String zkRoot;
  private ZooKeeper zk;

  private final String zkDir = System.getProperty("zk_dir", "/tmp/zk_in_tests");
  private Factory standaloneServerFactory;

  private int zkClientPort;

  private boolean startedZk = false;

  public ZkTestCase() {
    super();
    zkRoot = "/" + getClass().getSimpleName();
  }

  private int setupZkServer() throws Exception {
    File zkDirFile = new File(zkDir);
    FileUtils.deleteDirectory(zkDirFile);
    zkDirFile.mkdirs();

    ZooKeeperServer server = new ZooKeeperServer(zkDirFile, zkDirFile, TICK_TIME);

    int clientPort = 2000;
    while (true) {
      try {
        standaloneServerFactory =
          new NIOServerCnxn.Factory(clientPort);
      } catch (BindException e) {
        LOG.trace("Failed binding ZK Server to client port: " + clientPort);
        //this port is already in use. try to use another
        clientPort++;
        continue;
      }
      LOG.trace("Succeeded in binding ZK Server to client port " + clientPort);
      break;
    }
    standaloneServerFactory.startup(server);

    if (!waitForServerUp(clientPort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for startup of standalone server");
    }
    startedZk = true;
    return clientPort;
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

    zkClientPort = setupZkServer();

    zk = new ZooKeeper("127.0.0.1:" + zkClientPort, 1000000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {}
    });

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

  protected void create(String path) throws Exception {
    create(path, (byte[]) null);
  }

  protected void create(String path, String data) throws Exception {
    create(path, data.getBytes());
  }

  protected void create(String path, byte[] data) throws Exception {
    getZk().create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    shutdownZk();
  }

  public void shutdownZk() throws Exception {
    if (!startedZk) {
      return;
    }

    zk.close();
    zk = null;

    standaloneServerFactory.shutdown();
    if (!waitForServerDown(zkClientPort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }

    startedZk = false;
  }

  // XXX: From o.a.zk.t.ClientBase
  private boolean waitForServerDown(int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        Socket sock = new Socket("localhost", port);
        try {
          OutputStream outstream = sock.getOutputStream();
          outstream.write("stat".getBytes());
          outstream.flush();
        } finally {
          sock.close();
        }
      } catch (IOException e) {
        return true;
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
    try {
      zk.delete(path, -1);
    } catch (KeeperException.NotEmptyException e) {
      List<String> children = zk.getChildren(path, null);
      for (String child : children) {
        deleteNodeRecursively(path + "/" + child);
      }
      zk.delete(path, -1);
    } catch (KeeperException.NoNodeException e) {
      // Silently return if the node has already been deleted.
      return;
    }
  }
}
