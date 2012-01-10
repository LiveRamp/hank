package com.rapleaf.hank.client.async;

import com.rapleaf.hank.generated.PartitionServer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class TheoreticalLimit {

  private static final Logger LOG = Logger.getLogger(TheoreticalLimit.class);
  Random random = new Random();
  int queryPerThread;
  AtomicLong queryCount = new AtomicLong(0);
  TAsyncClientManager asyncClientManager;
  BlockingQueue<PartitionServer.AsyncClient> connectionPool;
  private boolean block;

  private class TheoreticalLimitRunnable implements Runnable {

    CountDownLatch countDownLatch;

    private class TheoreticalLimitCallback implements AsyncMethodCallback<PartitionServer.AsyncClient.get_call> {
      PartitionServer.AsyncClient client;

      public TheoreticalLimitCallback(PartitionServer.AsyncClient client) {
        this.client = client;
      }

      @Override
      public void onComplete(PartitionServer.AsyncClient.get_call response) {
        queryCount.incrementAndGet();
        try {
          connectionPool.put(client);
        } catch (InterruptedException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        if (block) {
          countDownLatch.countDown();
        }
      }

      @Override
      public void onError(Exception exception) {
        LOG.error("Error: ", exception);
      }
    }

    @Override
    public void run() {
      try {
        int domainId = 1;
        ByteBuffer key = ByteBuffer.wrap("test".getBytes());

        for (int i = 0; i < queryPerThread; ++i) {
          PartitionServer.AsyncClient client = connectionPool.take();

          TheoreticalLimitCallback callback = new TheoreticalLimitCallback(client);
          if (block) {
            countDownLatch = new CountDownLatch(1);
          }
          client.get(domainId, key, callback);
          if (block) {
            countDownLatch.await();
          }
        }
      } catch (TException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }

  void test(String[] args) throws InterruptedException, IOException {
    if (args.length != 3) {
      System.out.println("Missing argument");
      return;
    }
    int nbThread = Integer.parseInt(args[0]);
    queryPerThread = Integer.parseInt(args[1]);
    int nbConnection = Integer.parseInt(args[2]);
    block = Boolean.parseBoolean(args[3]);

    System.out.println("NbThread " + nbThread + ", QueryPerThread " + queryPerThread + ", NbConnection " + nbConnection);

    asyncClientManager = new TAsyncClientManager();
    connectionPool = new LinkedBlockingQueue<PartitionServer.AsyncClient>();
    for (int i = 0; i < nbConnection; ++i) {
      TNonblockingTransport transport = new TNonblockingSocket(random.nextInt(2) == 0 ? "hank04.rapleaf.com" : "hank05.rapleaf.com", 12345, 0);
      TProtocolFactory factory = new TCompactProtocol.Factory();
      PartitionServer.AsyncClient client = new PartitionServer.AsyncClient(factory, asyncClientManager, transport);
      connectionPool.put(client);
    }

    LinkedList<Thread> threads = new LinkedList<Thread>();
    long start = System.nanoTime();
    for (int i = 0; i < nbThread; ++i) {
      Thread thread = new Thread(new TheoreticalLimitRunnable(), "Runner");
      thread.run();
      threads.addLast(thread);
    }

    for (Thread thread : threads) {
      thread.join();
    }
    float elapsedS = ((float) (System.nanoTime() - start)) / 1000000000;
    System.out.println("QPS is " + ((float) queryCount.get() / elapsedS) + " (" + queryCount.get() + ", " + elapsedS + ")")
    ;
  }

  public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException, IOException {
    TheoreticalLimit theoreticalLimit = new TheoreticalLimit();
    theoreticalLimit.test(args);
  }

}
