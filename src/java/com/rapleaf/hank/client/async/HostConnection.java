package com.rapleaf.hank.client.async;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartitionServer;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class HostConnection implements WatchedNodeListener<HostState> {

  private static final Logger LOG = Logger.getLogger(HostConnection.class);

  private final int establishConnectionTimeoutMs;
  private final int queryTimeoutMs;
  private final int bulkQueryTimeoutMs;
  private final Host host;
  private final Runnable connectionListener;
  private final TAsyncClientManager asyncClientManager;
  private TNonblockingTransport transport;
  private PartitionServer.Client client;

  // Note: state is volatile because it is read and written by different threads and we want to avoid the overhead
  // of synchronize. Missing an update is fine in this case.
  private volatile HostConnectionState state = HostConnectionState.DISCONNECTED;

  // Standby: we are waiting for the host to be SERVING
  private volatile boolean isStandby = true;

  private volatile boolean isBusy = false;

  private static enum HostConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED
  }

  public static class fake_call_mock extends PartitionServer.AsyncClient.get_call {
    HankResponse response;

    public fake_call_mock(HankResponse response) throws TException {
      super(0, null, null, null, null, null);
      this.response = response;
    }

    public HankResponse getResult() throws org.apache.thrift.TException {
      return response;
    }
  }

  // A timeout of 0 means no timeout
  public HostConnection(Host host,
                        Runnable connectionListener,
                        TAsyncClientManager asyncClientManager,
                        int establishConnectionTimeoutMs,
                        int queryTimeoutMs,
                        int bulkQueryTimeoutMs) throws TException, IOException {
    this.host = host;
    this.connectionListener = connectionListener;
    this.asyncClientManager = asyncClientManager;
    this.establishConnectionTimeoutMs = establishConnectionTimeoutMs;
    this.queryTimeoutMs = queryTimeoutMs;
    this.bulkQueryTimeoutMs = bulkQueryTimeoutMs;
    host.setStateChangeListener(this);
    onWatchedNodeChange(host.getState());
    if (this.establishConnectionTimeoutMs != 0) {
      throw new NotImplementedException("ConnectionTimeout is not implemented. Set it to 0.");
    }
  }

  public void get(int domainId, ByteBuffer key, HostConnectionGetCallback resultHandler) {
    checkValidState();
    try {
      //client.setTimeout(queryTimeoutMs);
      HankResponse response = client.get(domainId, key/*, resultHandler*/);
      resultHandler.onComplete(new fake_call_mock(response));
    } catch (TException e) {
      resultHandler.onError(e);
    }
  }

  public void getBulk(int domainId, List<ByteBuffer> keys, HostConnectionGetBulkCallback resultHandler) {
    checkValidState();
    try {
      //client.setTimeout(bulkQueryTimeoutMs);
      client.getBulk(domainId, keys/*, resultHandler*/);
    } catch (TException e) {
      resultHandler.onError(e);
    }
  }

  Host getHost() {
    return host;
  }

  public boolean isBusy() {
    return isBusy;
  }

  public void setIsBusy(boolean isBusy) {
    this.isBusy = isBusy;
  }

  boolean isStandby() {
    return isStandby;
  }

  protected boolean isDisconnected() {
    return state == HostConnectionState.DISCONNECTED;
  }

  protected boolean isConnecting() {
    return state == HostConnectionState.CONNECTING;
  }

  protected boolean isConnected() {
    return state == HostConnectionState.CONNECTED;
  }

  public synchronized void setConnecting() {
    state = HostConnectionState.CONNECTING;
  }

  public synchronized void attemptConnect() throws IOException {
    if (state == HostConnectionState.CONNECTING && !isStandby) {
      connectNotSynchronized();
    }
  }

  public synchronized void attemptDisconnect() {
    disconnectNotSynchronized();
  }

  private void connectNotSynchronized() throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Trying to connect to " + host.getAddress());
    }
    // Use connection timeout to connect
    TFramedTransport transport = new TFramedTransport(new TSocket(host.getAddress().getHostName(),
        host.getAddress().getPortNumber(),
        establishConnectionTimeoutMs));
    try {
      transport.open();
    } catch (TTransportException e) {
      LOG.error("Failed to establish connection to host " + host.getAddress(), e);
      throw new IOException("Failed to establish connection to host " + host.getAddress(), e);
    }
    TProtocol proto = new TCompactProtocol(transport);
    client = new PartitionServer.Client(proto);
    state = HostConnectionState.CONNECTED;
    if (connectionListener != null) {
      connectionListener.run();
    }
  }

  private void disconnectNotSynchronized() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Disconnecting " + host.getAddress().getHostName() + ":" + host.getAddress().getPortNumber());
    }
    if (transport != null) {
      transport.close();
    }
    transport = null;
    client = null;
    state = HostConnectionState.DISCONNECTED;
  }

  private void checkValidState() {
    if (client == null || isStandby() || !isConnected() || !isBusy()) {
      throw new IllegalStateException();
    }
  }

  @Override
  public void onWatchedNodeChange(HostState hostState) {
    if (hostState != null && hostState == HostState.SERVING) {
      isStandby = false;
      if (connectionListener != null) {
        connectionListener.run();
      }
    } else {
      isStandby = true;
    }
  }

  @Override
  public String toString() {
    return "AsyncHostConnection [host=" + host.getAddress() + ", state=" + state
        + ", busy=" + isBusy + ", standby=" + isStandby + "]";
  }

  protected class IllegalStateException extends RuntimeException {
    public IllegalStateException() {
      super("Invalid state for request: " + HostConnection.this);
    }
  }
}
