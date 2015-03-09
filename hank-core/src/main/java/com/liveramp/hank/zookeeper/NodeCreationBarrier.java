package com.liveramp.hank.zookeeper;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class NodeCreationBarrier implements Watcher {

  private static final Logger LOG = LoggerFactory.getLogger(NodeCreationBarrier.class);

  private boolean waiting = true;
  private final String nodePath;
  private final ZooKeeperPlus zk;

  // Will block until specified node is created or connection is lost
  public static void block(ZooKeeperPlus zk, String nodePath) throws InterruptedException, KeeperException {
    new NodeCreationBarrier(zk, nodePath).block();
  }

  // Will block until specified node is created or connection is lost or timeout is exceeded
  public static void block(ZooKeeperPlus zk, String nodePath, int timeoutMS) throws InterruptedException, KeeperException {
    new NodeCreationBarrier(zk, nodePath).block(timeoutMS);
  }

  public NodeCreationBarrier(ZooKeeperPlus zk, String nodePath) throws InterruptedException, KeeperException {
    this.nodePath = nodePath;
    this.zk = zk;
  }

  // Will block until specified node is created or connection is lost
  public void block() throws InterruptedException, KeeperException {
    block(0);
  }

  // Will block until specified node is created or connection is lost or timeout is exceeded
  public synchronized void block(int timeoutMS) throws InterruptedException, KeeperException {
    // Wait only if it doesn't exist
    if (waiting && zk.exists(nodePath, this) == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Wait for creation of node " + nodePath);
      }
      if (timeoutMS != 0) {
        this.wait(timeoutMS);
      } else {
        this.wait();
      }
    }
  }

  @Override
  public synchronized void process(WatchedEvent watchedEvent) {
    // If we lose synchronization, stop waiting
    if (watchedEvent.getState() != Event.KeeperState.SyncConnected) {
      waiting = false;
    } else {
      // If the node has been created, stop waiting
      if (watchedEvent.getType() == Event.EventType.NodeCreated) {
        waiting = false;
      }
    }
    // Notify threads to check waiting flag
    notifyAll();
  }
}
