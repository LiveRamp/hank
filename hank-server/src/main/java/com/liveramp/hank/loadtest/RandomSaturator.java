package com.liveramp.hank.loadtest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.client.HankSmartClient;
import com.liveramp.hank.client.HankSmartClientOptions;
import com.liveramp.hank.config.ClientConfigurator;
import com.liveramp.hank.config.yaml.YamlClientConfigurator;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.util.CommandLineChecker;

public class RandomSaturator {

  private static final Logger LOG = LoggerFactory.getLogger(RandomSaturator.class);

  public static class LoadThread extends Thread {
    private final HankSmartClient client;
    private final String domainName;
    private final int numReqs;
    private final int keyLength;
    public int hits;
    private List<Long> times = new ArrayList<Long>();
    private final ReadWriteLock lock;
    private boolean ready = false;
    private long runStart;
    private long runEnd;

    public LoadThread(HankSmartClient client,
                      String domainName,
                      int threadNum,
                      int numReqs,
                      int keyLength,
                      ReadWriteLock lock) throws Exception {
      super("LoadThread #" + threadNum);
      this.domainName = domainName;
      this.numReqs = numReqs;
      this.keyLength = keyLength;
      this.lock = lock;
      this.client = client;
    }

    @Override
    public void run() {
      ByteBuffer[] keys = new ByteBuffer[numReqs];
      Random r = new Random();
      for (int i = 0; i < numReqs; i++) {
        final byte[] bs = new byte[keyLength];
        keys[i] = ByteBuffer.wrap(bs);
        r.nextBytes(bs);
      }

      ready = true;
      lock.readLock().lock();
      runStart = System.currentTimeMillis();

      for (int i = 0; i < numReqs; i++) {
        long start = System.currentTimeMillis();
        final HankResponse resp = client.get(domainName, keys[i]);
        if (resp.is_set_xception()) {
          LOG.error(resp.toString());
        }
        long end = System.currentTimeMillis();
        times.add(end - start);
        if (resp.isSet(HankResponse._Fields.VALUE)) {
          hits++;
        }
      }
      runEnd = System.currentTimeMillis();
    }
  }

  public static void main(String[] args) throws Exception {
    String[] expectedArguments = {"client config file path",
        "ring group name",
        "domain name",
        "num threads",
        "num requests",
        "key length",
        "num connections per host",
        "query max num tries",
        "try lock connection timeout ms",
        "establish connection timeout ms",
        "query timeout ms",
        "bulk query timeout ms"};
    CommandLineChecker.check(args, expectedArguments, RandomSaturator.class);

    org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

    // parse opts
    ClientConfigurator configurator = new YamlClientConfigurator(args[0]);
    String ringGroupName = args[1];
    String domainName = args[2];
    int numThreads = Integer.parseInt(args[3]);
    int numReqs = Integer.parseInt(args[4]);
    int keyLength = Integer.parseInt(args[5]);
    int numConnectionsPerHost = Integer.parseInt(args[6]);
    int queryMaxNumTries = Integer.parseInt(args[7]);
    int tryLockConnectionTimeoutMs = Integer.parseInt(args[8]);
    int establishConnectionTimeoutMs = Integer.parseInt(args[9]);
    int queryTimeoutMs = Integer.parseInt(args[10]);
    int bulkQueryTimeoutMs = Integer.parseInt(args[11]);

    // set up the delay lock
    ReadWriteLock lock = new ReentrantReadWriteLock();
    lock.writeLock().lock();

    final HankSmartClient client = new HankSmartClient(configurator.createCoordinator(), ringGroupName, new HankSmartClientOptions()
        .setNumConnectionsPerHost(numConnectionsPerHost)
        .setQueryMaxNumTries(queryMaxNumTries)
        .setTryLockConnectionTimeoutMs(tryLockConnectionTimeoutMs)
        .setEstablishConnectionTimeoutMs(establishConnectionTimeoutMs)
        .setQueryTimeoutMs(queryTimeoutMs)
        .setBulkQueryTimeoutMs(bulkQueryTimeoutMs));

    // instantiate all the threads
    List<LoadThread> threads = new ArrayList<LoadThread>();
    for (int i = 0; i < numThreads; i++) {
      final LoadThread lt = new LoadThread(client, domainName, i, numReqs, keyLength, lock);
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
    }
    System.err.println("Total throughput: " + totalThroughput + " req/s");
  }
}
