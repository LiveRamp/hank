/**
 *  Copyright 2011 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.hank.performance;

import com.liveramp.hank.util.FormatUtils;
import com.liveramp.hank.util.HankTimer;
import com.liveramp.hank.util.HankTimerEventAggregator;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class RandomReadPerformance {

  private static final Logger LOG = LoggerFactory.getLogger(RandomReadPerformance.class);

  private static final int NUM_RANDOM_READS = 8 << 10;
  private static final int NUM_RANDOM_READ_THREADS = 8;
  private static final int NUM_OPTIONS = 1;

  public static void main(String[] args) throws IOException, InterruptedException {
    int randomReadBufferSize = Integer.valueOf(args[0]);
    long totalRandomReads = NUM_RANDOM_READS * NUM_RANDOM_READ_THREADS;

    File[] testFiles = new File[args.length - NUM_OPTIONS];
    for (int i = NUM_OPTIONS; i < args.length; ++i) {
      testFiles[i - NUM_OPTIONS] = new File(args[i]);
      LOG.info("Using test file: " + testFiles[i - NUM_OPTIONS].getAbsolutePath());
    }

    Thread[] threads = new Thread[NUM_RANDOM_READ_THREADS];
    for (int i = 0; i < NUM_RANDOM_READ_THREADS; ++i) {
      threads[i] = new Thread(new RandomReadsRunnable(testFiles, randomReadBufferSize));
    }

    LOG.info("Calculating time taken to perform " + totalRandomReads
        + " random " + FormatUtils.formatNumBytes(randomReadBufferSize) + " reads in "
        + NUM_RANDOM_READ_THREADS + " threads (" + NUM_RANDOM_READS + " random reads each)");

    long startTime = System.currentTimeMillis();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    long totalDuration = System.currentTimeMillis() - startTime;
    LOG.info("Total duration: " + totalDuration + " ms");
    LOG.info("Total throughput: " + ((double) totalRandomReads / (totalDuration / 1000.0)) + " random reads per second");
    LOG.info("Total throughput: " + FormatUtils.formatNumBytes((long) ((totalRandomReads * randomReadBufferSize) / (totalDuration / 1000.0))) + "/s");
  }

  private static class RandomReadsRunnable implements Runnable {

    private final FileChannel[] testChannels;
    private final HankTimerEventAggregator timerAggregator;
    private final int randomReadBufferSize;

    public RandomReadsRunnable(File[] testFiles, int randomReadBufferSize) throws FileNotFoundException {
      // Open file channels
      testChannels = new FileChannel[testFiles.length];
      for (int i = 0; i < testFiles.length; ++i) {
        testChannels[i] = new FileInputStream(testFiles[i]).getChannel();
      }
      timerAggregator = new HankTimerEventAggregator("Random reads", 1);
      this.randomReadBufferSize = randomReadBufferSize;
    }

    @Override
    public void run() {
      try {
        Random random = new Random();
        // Perform random reads
        byte[] readBufferArray = new byte[randomReadBufferSize];
        ByteBuffer readBuffer = ByteBuffer.wrap(readBufferArray);
        HankTimer timer = timerAggregator.getTimer();
        for (int i = 0; i < NUM_RANDOM_READS; ++i) {
          readBuffer.clear();
          FileChannel testChannel = testChannels[i % testChannels.length];
          long randomPosition = Math.abs(random.nextLong()) % (testChannel.size() - randomReadBufferSize);
          testChannel.position(randomPosition)
              .read(readBuffer);
        }
        timerAggregator.add(timer, NUM_RANDOM_READS);
        // Close file channels
        for (FileChannel testChannel : testChannels) {
          testChannel.close();
        }
      } catch (Exception e) {
        LOG.error("Failed to perform random reads", e);
      }
    }
  }
}
