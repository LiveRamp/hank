package com.rapleaf.hank.client;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.generated.PartitionServer;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncHostConnection {

  private static final Logger LOG = Logger.getLogger(AsyncHostConnection.class);

  private final int establishConnectionTimeoutMs;
  private final int queryTimeoutMs;
  private final int bulkQueryTimeoutMs;
  private SocketChannel socket;
  private TNonblockingTransport transport;
  private PartitionServer.AsyncClient client;
  private final Host host;
  protected final ReentrantLock lock = new ReentrantLock(true); // Use a fair ReentrantLock
  private HostConnectionState state = HostConnectionState.DISCONNECTED;
  private final TAsyncClientManager asyncClientManager;

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

  Host getHost() {
    return host;
  }

  boolean isAvailable() {
    return state != HostConnectionState.STANDBY;
  }

  private boolean isDisconnected() {
    return state == HostConnectionState.DISCONNECTED;
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
    socket = SocketChannel.open(new InetSocketAddress(host.getAddress().getHostName(),
                                host.getAddress().getPortNumber()));
    transport = new TNonblockingSocket(socket);
    try {
      transport.open();
      // Set socket timeout to regular mode
      setSocketTimeout(queryTimeoutMs);
    } catch (TTransportException e) {
      LOG.error("Failed to establish connection to host " + host.getAddress(), e);
      disconnect();
      throw new IOException("Failed to establish connection to host " + host.getAddress(), e);
    }
    TProtocolFactory factory = new TCompactProtocol.Factory();
    client = new PartitionServer.AsyncClient(factory, asyncClientManager, transport);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Connection to " + host.getAddress() + " opened.");
    }
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
}
