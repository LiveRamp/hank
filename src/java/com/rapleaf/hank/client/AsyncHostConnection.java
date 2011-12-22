package com.rapleaf.hank.client;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.generated.PartitionServer;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class AsyncHostConnection implements WatchedNodeListener<HostState> {

  private static final Logger LOG = Logger.getLogger(AsyncHostConnection.class);

  private final int establishConnectionTimeoutMs;
  private final int queryTimeoutMs;
  private final int bulkQueryTimeoutMs;
  private final TAsyncClientManager asyncClientManager;
  private final Host host;
  private SocketChannel socket;
  private TNonblockingTransport transport;
  private PartitionServer.AsyncClient client;
  // Note: state is volatile because it is read and written by different threads and we want to avoid the overhead
  // of synchronize. Missing an update is fine in this case.
  private volatile HostConnectionState state = HostConnectionState.DISCONNECTED;


  private static enum HostConnectionState {
    CONNECTED,
    DISCONNECTED,
    // STANDBY: we are waiting for the host to be SERVING
    STANDBY
  }

  // A timeout of 0 means no timeout
  public AsyncHostConnection(Host host,
                             TAsyncClientManager asyncClientManager,
                             int establishConnectionTimeoutMs,
                             int queryTimeoutMs,
                             int bulkQueryTimeoutMs) throws TException, IOException {
    this.host = host;
    this.establishConnectionTimeoutMs = establishConnectionTimeoutMs;
    this.queryTimeoutMs = queryTimeoutMs;
    this.bulkQueryTimeoutMs = bulkQueryTimeoutMs;
    this.asyncClientManager = asyncClientManager;
  }

  public void get(int domainId, ByteBuffer key, HostConnectionGetCallback resultHandler) throws TException {
    client.get(domainId, key, resultHandler);
  }

  public void getBulk(int domainId, List<ByteBuffer> keys, HostConnectionGetBulkCallback resultHandler) throws TException {
    client.getBulk(domainId, keys, resultHandler);
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

  public synchronized void connect() throws IOException {
    connectNotSynchronized();
  }

  private synchronized void reconnect() throws IOException {
    disconnectNotSynchronized();
    connectNotSynchronized();
  }

  private synchronized void disconnectAndStandby() {
    disconnectNotSynchronized();
    state = HostConnectionState.STANDBY;
  }

  private void connectNotSynchronized() throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Trying to connect to " + host.getAddress());
    }
    // Use connection timeout to connect
    socket = SocketChannel.open(new InetSocketAddress(host.getAddress().getHostName(),
        host.getAddress().getPortNumber()));
    transport = new TNonblockingSocket(socket);
    try {
      transport.open();
      // Set socket timeout to regular mode
      setSocketTimeout(queryTimeoutMs);
    } catch (TTransportException e) {
      LOG.error("Failed to establish connection to host " + host.getAddress(), e);
      disconnectNotSynchronized();
      throw new IOException("Failed to establish connection to host " + host.getAddress(), e);
    }
    TProtocolFactory factory = new TCompactProtocol.Factory();
    client = new PartitionServer.AsyncClient(factory, asyncClientManager, transport);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Connection to " + host.getAddress() + " opened.");
    }
    state = HostConnectionState.CONNECTED;
  }

  private void disconnectNotSynchronized() {
    if (transport != null) {
      transport.close();
    }
    socket = null;
    transport = null;
    client = null;
    state = HostConnectionState.DISCONNECTED;
  }

  private void setSocketTimeout(int timeout) throws IOException {
    if (socket != null) {
      try {
        socket.socket().setSoTimeout(timeout);
      } catch (SocketException e) {
        throw new IOException("Failed to set socket timeout to " + timeout, e);
      }
    }
  }

  @Override
  public void onWatchedNodeChange(HostState hostState) {
    if (hostState != null && hostState == HostState.SERVING) {
      // Reconnect
      try {
        reconnect();
      } catch (IOException e) {
        LOG.error("Error connecting to host " + host.getAddress(), e);
      }
    } else {
      // Disconnect and standby
      disconnectAndStandby();
    }
  }
}
