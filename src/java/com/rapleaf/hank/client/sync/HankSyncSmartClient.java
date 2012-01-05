package com.rapleaf.hank.client.sync;

import com.rapleaf.hank.client.GetBulkCallback;
import com.rapleaf.hank.client.GetCallback;
import com.rapleaf.hank.client.HankSmartClientIface;
import com.rapleaf.hank.client.async.HankAsyncSmartClient;
import com.rapleaf.hank.config.HankSmartClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class HankSyncSmartClient implements HankSmartClientIface {

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
    asyncSmartClient = new HankAsyncSmartClient(
            coordinator,
            ringGroupName,
            numConnectionsPerHost,
            queryMaxNumTries,
            /*establishConnectionTimeoutMs*/0,
            queryTimeoutMs,
            bulkQueryTimeoutMs);
  }

  private static class SyncGetCallback implements GetCallback {

    private HankResponse response;
    private final CountDownLatch completionBarrier = new CountDownLatch(1);

    @Override
    public void onComplete(HankResponse response) {
      this.response = response;
      completionBarrier.countDown();
    }
  }

  private static class SyncGetBulkCallback implements GetBulkCallback {

    private HankBulkResponse response;
    private final CountDownLatch completionBarrier = new CountDownLatch(1);

    @Override
    public void onComplete(HankBulkResponse response) {
      this.response = response;
      completionBarrier.countDown();
    }
  }

  @Override
  public HankResponse get(String domainName, ByteBuffer key) {
    SyncGetCallback callback = new SyncGetCallback();
    try {
      asyncSmartClient.get(domainName, key, callback);
    } catch (TException e) {
      return HankResponse.xception(HankException.internal_error("GET throws an exception"));
    }
    try {
      callback.completionBarrier.await();
      return callback.response;
    } catch (InterruptedException e) {
      return INTERRUPTED_GET;
    }
  }

  @Override
  public HankBulkResponse getBulk(String domainName, List<ByteBuffer> keys) {
    SyncGetBulkCallback callback = new SyncGetBulkCallback();
    try {
      asyncSmartClient.getBulk(domainName, keys, callback);
    } catch (TException e) {
      return HankBulkResponse.xception(HankException.internal_error("GET throws an exception"));
    }
    try {
      callback.completionBarrier.await();
      return callback.response;
    } catch (InterruptedException e) {
      return INTERRUPTED_GET_BULK;
    }
  }

  @Override
  public void stop() {
    asyncSmartClient.stop();
  }

}
