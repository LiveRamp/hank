package com.rapleaf.hank.client;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.generated.PartitionServer;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncHostConnection implements WatchedNodeListener<HostState> {

  private static final Logger LOG = Logger.getLogger(AsyncHostConnection.class);

  private final int tryLockTimeoutMs;
  private final int establishConnectionTimeoutMs;
  private final int queryTimeoutMs;
  private final int bulkQueryTimeoutMs;
  private TSocket socket;
  private TTransport transport;
  private PartitionServer.AsyncClient client;
  private final Host host;
  protected final ReentrantLock lock = new ReentrantLock(true); // Use a fair ReentrantLock
  private HostConnectionState state = HostConnectionState.DISCONNECTED;

  private static enum HostConnectionState {
    CONNECTED,
    DISCONNECTED,
    // STANDBY: we are waiting for the host to be SERVING
    STANDBY
  }

  // A timeout of 0 means no timeout
  public AsyncHostConnection(Host host,
                        int tryLockTimeoutMs,
                        int establishConnectionTimeoutMs,
                        int queryTimeoutMs,
                        int bulkQueryTimeoutMs) throws TException, IOException {
    this.host = host;
    this.tryLockTimeoutMs = tryLockTimeoutMs;
    this.establishConnectionTimeoutMs = establishConnectionTimeoutMs;
    this.queryTimeoutMs = queryTimeoutMs;
    this.bulkQueryTimeoutMs = bulkQueryTimeoutMs;
    host.setStateChangeListener(this);
    onWatchedNodeChange(host.getState());
  }

  Host getHost() {
    return host;
  }

  boolean isAvailable() {
    return state != HostConnectionState.STANDBY;
  }

  private boolean isDisconnected() {
    return state == HostConnectionState.DISCONNECTED;
  }

  private void lock() {
    lock.lock();
  }

  private void unlock() {
    lock.unlock();
  }

  boolean tryLockRespectingFairness() {
    try {
      // Note: tryLock() does not respect fairness, using tryLock(0, unit) instead
      return lock.tryLock(0, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      return false;
    }
  }

  private boolean tryLockWithTimeout() {
    // If configured timeout is 0, wait indefinitely
    if (tryLockTimeoutMs == 0) {
      lock();
      return true;
    }
    // Otherwise, perform a lock with timeout. If interrupted, simply report that we failed to lock.
    try {
      return lock.tryLock(tryLockTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      return false;
    }
  }

  private void disconnect() {
    if (transport != null) {
      transport.close();
    }
    socket = null;
    transport = null;
    client = null;
    state = HostConnectionState.DISCONNECTED;
  }

  private void connect() throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Trying to connect to " + host.getAddress());
    }
    // Use connection timeout to connect
    socket = new TSocket(host.getAddress().getHostName(),
        host.getAddress().getPortNumber(),
        establishConnectionTimeoutMs);
    transport = new TFramedTransport(socket);
    try {
      transport.open();
      // Set socket timeout to regular mode
      setSocketTimeout(queryTimeoutMs);
    } catch (TTransportException e) {
      LOG.error("Failed to establish connection to host " + host.getAddress(), e);
      disconnect();
      throw new IOException("Failed to establish connection to host " + host.getAddress(), e);
    }
    TProtocol proto = new TCompactProtocol(transport);
    client = new PartitionServer.Client(proto);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Connection to " + host.getAddress() + " opened.");
    }
    state = HostConnectionState.CONNECTED;
  }

  private void setSocketTimeout(int timeout) {
    if (socket != null) {
      socket.setTimeout(timeout);
    }
  }

  @Override
  public void onWatchedNodeChange(HostState hostState) {
    if (hostState != null && hostState == HostState.SERVING) {
      // Reconnect
      lock();
      try {
        disconnect();
        try {
          connect();
        } catch (IOException e) {
          LOG.error("Error connecting to host " + host.getAddress(), e);
        }
      } finally {
        unlock();
      }
    } else {
      // Disconnect and standby
      lock();
      try {
        disconnect();
        state = HostConnectionState.STANDBY;
      } finally {
        unlock();
      }
    }
  }
}
