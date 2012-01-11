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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class TheoreticalLimit {

  private static final Logger LOG = Logger.getLogger(TheoreticalLimit.class);

  CountDownLatch queryCount;
  ConcurrentLinkedQueue<Long> latencies;

  // Connection pools
  BlockingQueue<PartitionServer.AsyncClient> connectionPool;
  BlockingQueue<PartitionServer.Client> directConnectionPool;

  // Options
  private boolean isSync;
  private boolean useDirectClient;
  private int nbConnection;
  private int nbThread;
  private int nbManager;
  private int queryPerThread;

  private class TheoreticalLimitRunnable implements Runnable {

    CountDownLatch countDownLatch;

    private class TheoreticalLimitCallback implements AsyncMethodCallback<PartitionServer.AsyncClient.get_call> {
      PartitionServer.AsyncClient client;
      long start;

      public TheoreticalLimitCallback(PartitionServer.AsyncClient client) {
        this.client = client;
        this.start = System.nanoTime();
      }

      @Override
      public void onComplete(PartitionServer.AsyncClient.get_call response) {
        queryCount.countDown();
        try {
          connectionPool.put(client);
        } catch (InterruptedException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        if (isSync) {
          countDownLatch.countDown();
        }
        long latency = Math.abs(System.nanoTime() - start);
        latencies.add(latency);
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
            if (isSync) {
              countDownLatch = new CountDownLatch(1);
            }
            client.get(domainId, key, callback);
            if (isSync) {
              countDownLatch.await();
            }
          }
        } else {
          PartitionServer.Client client = directConnectionPool.take();
          for (int i = 0; i < queryPerThread; ++i) {
            long start = System.nanoTime();
            client.get(domainId, key);
            queryCount.countDown();
            long latency = Math.abs(System.nanoTime() - start);
            latencies.add(latency);
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
    parseCommandLine(args);
    populateConnections();

    int queryTotal = nbThread * queryPerThread;
    // Stop condition
    queryCount = new CountDownLatch(queryTotal);

    latencies = new ConcurrentLinkedQueue<Long>();
    LinkedList<Thread> threads = new LinkedList<Thread>();

    long qpsStart = System.nanoTime();

    for (int i = 0; i < nbThread; ++i) {
      Thread thread = new Thread(new TheoreticalLimitRunnable(), "Runner");
      thread.start();
      threads.addLast(thread);
    }

    for (Thread thread : threads) {
      thread.join();
    }

    queryCount.await();

    float elapsedS = Math.abs(((float) (System.nanoTime() - qpsStart)) / 1000000000);
    System.out.println("QPS is " + ((float) queryTotal / elapsedS) + " (" + queryTotal + ", " + elapsedS + ")");

    for (Long l : latencies) {
      System.out.println(l / 1000000);
    }
  }

  private void parseCommandLine(String[] args) {
    if (args.length == 0) {
      argumentError();
    } else if (args[0].equals("direct")) {
      if (args.length != 3) {
        argumentError();
      } else {
        useDirectClient = true;
        nbThread = Integer.parseInt(args[1]);
        queryPerThread = Integer.parseInt(args[2]);

        System.out.println("ClientType " + args[0] + ", NbThread " + nbThread + ", QueryPerThread " + queryPerThread);
      }
    } else if (args[0].equals("sync") || args.equals("async")) {
      if (args.length != 5) {
        argumentError();
      } else {
        isSync = args[0].equals("sync");
        nbThread = Integer.parseInt(args[1]);
        queryPerThread = Integer.parseInt(args[2]);
        nbConnection = Integer.parseInt(args[3]);
        nbManager = Integer.parseInt(args[4]);

        System.out.println("ClientType " + args[0] + ", NbThread " + nbThread + ", QueryPerThread " + queryPerThread + ", NbConnection " + nbConnection + ", NbManager " + nbManager);
      }
    } else {
      argumentError();
    }

  }

  private void argumentError() {
    System.out.println("direct <nbThread> <queryPerThread>");
    System.out.println("async|sync <nbThread> <queryPerThread> <nbConnection> <nbManager>");
    throw new RuntimeException("Bad Command line arguments");
  }

  private void populateConnections() throws IOException, InterruptedException {
    String[] hostnames = {"hank04.rapleaf.com", "hank05.rapleaf.com"};
    Integer[] ports = {12345, 12345};

    if (!useDirectClient) {
      ArrayList<TAsyncClientManager> asyncClientManagers = new ArrayList<TAsyncClientManager>();
      for (int i = 0; i < nbManager; ++i) {
        asyncClientManagers.add(new TAsyncClientManager());
      }
      connectionPool = new LinkedBlockingQueue<PartitionServer.AsyncClient>();
      for (int i = 0; i < nbConnection; ++i) {
        String hostname = hostnames[i % hostnames.length];
        int port = ports[i % hostnames.length];
        TNonblockingTransport transport = new TNonblockingSocket(hostname, port, 0);
        TProtocolFactory factory = new TCompactProtocol.Factory();
        TAsyncClientManager manager = asyncClientManagers.get(i % asyncClientManagers.size());
        PartitionServer.AsyncClient client = new PartitionServer.AsyncClient(factory, manager, transport);
        connectionPool.put(client);
      }
    } else {
      directConnectionPool = new LinkedBlockingQueue<PartitionServer.Client>();
      for (int i = 0; i < nbThread; ++i) {
        try {
          String hostname = hostnames[i % hostnames.length];
          int port = ports[i % hostnames.length];
          TTransport transport = new TFramedTransport(new TSocket(hostname, port, 0));
          transport.open();
          TProtocol proto = new TCompactProtocol(transport);
          PartitionServer.Client client = new PartitionServer.Client(proto);
          directConnectionPool.put(client);
        } catch (TTransportException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException, IOException {
    TheoreticalLimit theoreticalLimit = new TheoreticalLimit();
    theoreticalLimit.test(args);
  }

}
