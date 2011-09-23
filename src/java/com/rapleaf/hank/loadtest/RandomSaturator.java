package com.rapleaf.hank.loadtest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.rapleaf.hank.client.HankSmartClient;
import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.generated.HankResponse;

public class RandomSaturator {
  public static class LoadThread extends Thread {
    private final HankSmartClient client;
    private final String domainName;
    private final int numReqs;
    private final int numBytes;
    public int hits;
    private List<Long> times = new ArrayList<Long>();
    private final ReadWriteLock lock;
    private boolean ready = false;
    private long runStart;
    private long runEnd;

    public LoadThread(HankSmartClient client,
        String domainName,
        int msBetweenRequests,
        int threadNum,
        int numReqs,
        int numBytes,
        ReadWriteLock lock) throws Exception {
      super("LoadThread #" + threadNum);
      this.domainName = domainName;
      this.numReqs = numReqs;
      this.numBytes = numBytes;
      this.lock = lock;
      this.client = client;
    }

    @Override
    public void run() {
      ByteBuffer[] keys = new ByteBuffer[numReqs];
      Random r = new Random();
      for (int i = 0; i < numReqs; i++) {
        final byte[] bs = new byte[numBytes];
        keys[i] = ByteBuffer.wrap(bs);
        r.nextBytes(bs);
      }

      ready = true;
      lock.readLock().lock();
      runStart = System.currentTimeMillis();

      try {
        for (int i = 0; i < numReqs; i++) {
          long start = System.currentTimeMillis();
          final HankResponse resp = client.get(domainName, keys[i]);
          long end = System.currentTimeMillis();
          times.add(end - start);
          if (resp.isSet(HankResponse._Fields.VALUE)) {
            hits++;
          }
        }
      } catch (TException t) {
        // Uh oh.
      }
      runEnd = System.currentTimeMillis();
    }
  }

  public static void main(String[] args) throws Exception {
    System.err.println("Usage: java "
        + RandomSaturator.class.getName()
        + " <client config file path> <ring group name> <domain name> <num threads> <ms between requests> <number of requests> <key length>");

    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

    // parse opts
    ClientConfigurator configurator = new YamlClientConfigurator(args[0]);
    String ringGroupName = args[1];
    String domainName = args[2];
    int numThreads = Integer.parseInt(args[3]);
    int msBetweenRequests = Integer.parseInt(args[4]);
    int numReqs = Integer.parseInt(args[5]);
    int numBytes = Integer.parseInt(args[6]);

    // set up the delay lock
    ReadWriteLock lock = new ReentrantReadWriteLock();
    lock.writeLock().lock();

    final HankSmartClient client = new HankSmartClient(configurator.createCoordinator(), ringGroupName, numThreads*3);

    // instantiate all the threads
    List<LoadThread> threads = new ArrayList();
    for (int i = 0; i < numThreads; i++) {
      final LoadThread lt = new LoadThread(client, domainName, msBetweenRequests, i, numReqs, numBytes, lock);
      threads.add(lt);
      lt.start();
    }

    // wait for the threads to be ready to run
    boolean exit = false;
    while (!exit) {
      exit = true;
      for (LoadThread t : threads) {
        if (!t.ready) {
          exit = false;
        }
      }
      Thread.sleep(1000);
    }

    // release the delay lock
    lock.writeLock().unlock();

    // let all the threads run to completion
    for (LoadThread t : threads) {
      t.join();
    }

    // aggregate the results
    double totalThroughput = 0;

    for (LoadThread t : threads) {
      System.err.println("Thread " + t + " had " + t.hits + " hits");
      totalThroughput += t.times.size() / ((t.runEnd - t.runStart) / 1000.0);
      for (Long l : t.times) {
        System.out.println(l);
      }
    }
    System.err.println("Total throughput: " + totalThroughput + " req/s");
  }
}
