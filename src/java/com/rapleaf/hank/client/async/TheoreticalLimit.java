package com.rapleaf.hank.client.async;

import com.rapleaf.hank.generated.PartitionServer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
  CountDownLatch queryCount;
  BlockingQueue<PartitionServer.AsyncClient> connectionPool;
  BlockingQueue<PartitionServer.Client> directConnectionPool;
  private boolean block;
  private boolean useDirectClient;
  private int nbConnection;
  private int nbThread;
  private int nbManager;

  private class TheoreticalLimitRunnable implements Runnable {

    CountDownLatch countDownLatch;

    private class TheoreticalLimitCallback implements AsyncMethodCallback<PartitionServer.AsyncClient.get_call> {
      PartitionServer.AsyncClient client;

      public TheoreticalLimitCallback(PartitionServer.AsyncClient client) {
        this.client = client;
      }

      @Override
      public void onComplete(PartitionServer.AsyncClient.get_call response) {
        queryCount.countDown();
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

        if (!useDirectClient) {
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
        } else {
          PartitionServer.Client client = directConnectionPool.take();
          for (int i = 0; i < queryPerThread; ++i) {
            client.get(domainId, key);
            queryCount.countDown();
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
    if (args.length != 6) {
      System.out.println("Missing argument");
      return;
    }
    nbThread = Integer.parseInt(args[0]);
    queryPerThread = Integer.parseInt(args[1]);
    nbConnection = Integer.parseInt(args[2]);
    block = Boolean.parseBoolean(args[3]);
    useDirectClient = Boolean.parseBoolean(args[4]);
    nbManager = Integer.parseInt(args[5]);

    System.out.println("NbThread " + nbThread + ", QueryPerThread " + queryPerThread + ", NbConnection " + nbConnection);

    if (!useDirectClient) {
      ArrayList<TAsyncClientManager> asyncClientManagers = new ArrayList<TAsyncClientManager>();
      for (int i = 0; i < nbManager; ++i) {
        asyncClientManagers.add(new TAsyncClientManager());
      }
      connectionPool = new LinkedBlockingQueue<PartitionServer.AsyncClient>();
      for (int i = 0; i < nbConnection; ++i) {
        TNonblockingTransport transport = new TNonblockingSocket(random.nextInt(2) == 0 ? "hank04.rapleaf.com" : "hank05.rapleaf.com", 12345, 0);
        TProtocolFactory factory = new TCompactProtocol.Factory();
        PartitionServer.AsyncClient client = new PartitionServer.AsyncClient(factory, asyncClientManagers.get(random.nextInt(2)), transport);
        connectionPool.put(client);
      }
    } else {
      directConnectionPool = new LinkedBlockingQueue<PartitionServer.Client>();
      for (int i = 0; i < nbThread; ++i) {
        try {
          TTransport transport = new TFramedTransport(new TSocket(random.nextInt(2) == 0 ? "hank04.rapleaf.com" : "hank05.rapleaf.com", 12345, 0));
          transport.open();
          TProtocol proto = new TCompactProtocol(transport);
          PartitionServer.Client client = new PartitionServer.Client(proto);
          directConnectionPool.put(client);
        } catch (TTransportException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }

    int queryTotal = nbThread * queryPerThread;
    queryCount = new CountDownLatch(queryTotal);
    LinkedList<Thread> threads = new LinkedList<Thread>();
    long start = System.nanoTime();
    for (int i = 0; i < nbThread; ++i) {
      Thread thread = new Thread(new TheoreticalLimitRunnable(), "Runner");
      thread.start();
      threads.addLast(thread);
    }

    for (Thread thread : threads) {
      thread.join();
    }

    queryCount.await();

    float elapsedS = ((float) (System.nanoTime() - start)) / 1000000000;
    System.out.println("QPS is " + ((float) queryTotal / elapsedS) + " (" + queryTotal + ", " + elapsedS + ")");
  }

  public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException, IOException {
    TheoreticalLimit theoreticalLimit = new TheoreticalLimit();
    theoreticalLimit.test(args);
  }

}
