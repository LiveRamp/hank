package com.rapleaf.hank.client.sync;

import com.rapleaf.hank.client.GetBulkCallback;
import com.rapleaf.hank.client.GetCallback;
import com.rapleaf.hank.client.async.HankAsyncSmartClient;
import com.rapleaf.hank.config.HankSmartClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class HankSyncSmartClient implements SmartClient.Iface {

  private final HankAsyncSmartClient asyncSmartClient;

  private static final HankResponse INTERRUPTED_GET =
      HankResponse.xception(HankException.internal_error("Interrupted during GET"));
  private static final HankBulkResponse INTERRUPTED_GET_BULK =
      HankBulkResponse.xception(HankException.internal_error("Interrupted during GET BULK"));

  public HankSyncSmartClient(Coordinator coordinator,
                             HankSmartClientConfigurator configurator) throws IOException, TException {
    this(coordinator,
        configurator.getRingGroupName(),
        configurator.getNumConnectionsPerHost(),
        configurator.getQueryNumMaxTries(),
        configurator.getEstablishConnectionTimeoutMs(),
        configurator.getQueryTimeoutMs(),
        configurator.getBulkQueryTimeoutMs());
  }

  public HankSyncSmartClient(Coordinator coordinator,
                             String ringGroupName,
                             int numConnectionsPerHost,
                             int queryMaxNumTries,
                             int establishConnectionTimeoutMs,
                             int queryTimeoutMs,
                             int bulkQueryTimeoutMs) throws IOException, TException {
    asyncSmartClient = new HankAsyncSmartClient(coordinator, ringGroupName, numConnectionsPerHost,
        queryMaxNumTries, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs);
  }

  private static class SyncGetCallback implements GetCallback {

    private HankResponse response;
    private final Thread threadToInterrupt;

    public SyncGetCallback() {
      response = null;
      threadToInterrupt = Thread.currentThread();
    }

    @Override
    public void onComplete(HankResponse response) {
      this.response = response;
      threadToInterrupt.interrupt();
    }
  }

  private static class SyncGetBulkCallback implements GetBulkCallback {

    private HankBulkResponse response;
    private final Thread threadToInterrupt;

    public SyncGetBulkCallback() {
      response = null;
      threadToInterrupt = Thread.currentThread();
    }

    @Override
    public void onComplete(HankBulkResponse response) {
      this.response = response;
      threadToInterrupt.interrupt();
    }
  }

  @Override
  public HankResponse get(String domainName, ByteBuffer key) throws TException {
    SyncGetCallback callback = new SyncGetCallback();
    asyncSmartClient.get(domainName, key, callback);
    try {
      Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException e) {
      if (callback.response == null) {
        return INTERRUPTED_GET;
      } else {
        return callback.response;
      }
    }
    return INTERRUPTED_GET;
  }

  @Override
  public HankBulkResponse getBulk(String domainName, List<ByteBuffer> keys) throws TException {
    SyncGetBulkCallback callback = new SyncGetBulkCallback();
    asyncSmartClient.getBulk(domainName, keys, callback);
    try {
      Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException e) {
      if (callback.response == null) {
        return INTERRUPTED_GET_BULK;
      } else {
        return callback.response;
      }
    }
    return INTERRUPTED_GET_BULK;
  }
}
