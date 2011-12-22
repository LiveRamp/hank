package com.rapleaf.hank.client;

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
import java.util.concurrent.CountDownLatch;

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
        configurator.getTryLockConnectionTimeoutMs(),
        configurator.getEstablishConnectionTimeoutMs(),
        configurator.getQueryTimeoutMs(),
        configurator.getBulkQueryTimeoutMs());
  }

  public HankSyncSmartClient(Coordinator coordinator,
                             String ringGroupName,
                             int numConnectionsPerHost,
                             int queryMaxNumTries,
                             int tryLockConnectionTimeoutMs,
                             int establishConnectionTimeoutMs,
                             int queryTimeoutMs,
                             int bulkQueryTimeoutMs) throws IOException, TException {
    asyncSmartClient = new HankAsyncSmartClient(coordinator, ringGroupName, numConnectionsPerHost,
        queryMaxNumTries, tryLockConnectionTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs);
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
  public HankResponse get(String domainName, ByteBuffer key) throws TException {
    SyncGetCallback callback = new SyncGetCallback();
    asyncSmartClient.get(domainName, key, callback);
    try {
      callback.completionBarrier.await();
      return callback.response;
    } catch (InterruptedException e) {
      return INTERRUPTED_GET;
    }
  }

  @Override
  public HankBulkResponse getBulk(String domainName, List<ByteBuffer> keys) throws TException {
    SyncGetBulkCallback callback = new SyncGetBulkCallback();
    asyncSmartClient.getBulk(domainName, keys, callback);
    try {
      callback.completionBarrier.await();
      return callback.response;
    } catch (InterruptedException e) {
      return INTERRUPTED_GET_BULK;
    }
  }
}
