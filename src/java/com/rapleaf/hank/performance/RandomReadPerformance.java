/**
 *  Copyright 2011 Rapleaf
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

package com.rapleaf.hank.performance;

import com.rapleaf.hank.ui.UiUtils;
import com.rapleaf.hank.util.CommandLineChecker;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class RandomReadPerformance {

  private static final Logger LOG = Logger.getLogger(RandomReadPerformance.class);

  private static final int NUM_TEST_FILES = 16;
  private static final int RANDOM_BYTES_BUFFER_SIZE = 32 * (1 << 10);
  private static final int NUM_TEST_FILE_RANDOM_BYTES_BUFFERS = 32 * (1 << 10);
  private static final int NUM_RANDOM_READS = 8 << 10;
  private static final int NUM_RANDOM_READ_THREADS = 8;
  private static final int GET_TIMER_AGGREGATOR_WINDOW = NUM_RANDOM_READS;

  public static void main(String[] args) throws IOException, InterruptedException {
    CommandLineChecker.check(args, new String[]{"work_directory"}, RandomReadPerformance.class);
    Random random = new Random();
    long totalRandomReads = NUM_RANDOM_READS * NUM_RANDOM_READ_THREADS;

    String workDirectory = args[0];
    File workDirectoryFile = new File(workDirectory);
    if (workDirectoryFile.exists()) {
      throw new IOException("Work directory already exists: " + workDirectoryFile.getAbsolutePath());
    }
    if (!workDirectoryFile.mkdirs()) {
      throw new IOException("Failed to create directory: " + workDirectoryFile.getAbsolutePath());
    }

    // Create test files
    File[] testFiles = createTestFiles(workDirectoryFile, random);

    Thread[] threads = new Thread[NUM_RANDOM_READ_THREADS];
    for (int i = 0; i < NUM_RANDOM_READ_THREADS; ++i) {
      threads[i] = new Thread(new RandomReadsRunnable(testFiles));
    }

    LOG.info("Calculating time taken to perform " + totalRandomReads + " random reads in " + NUM_RANDOM_READ_THREADS + " threads (" + NUM_RANDOM_READS + " random reads each)");

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
    LOG.info("Total throughput: " + UiUtils.formatNumBytes((long) ((totalRandomReads * RANDOM_BYTES_BUFFER_SIZE) / (totalDuration / 1000.0))) + "/s");
  }

  private static File[] createTestFiles(File workDirectoryFile, Random random) throws IOException {
    byte[] randomBytes = new byte[RANDOM_BYTES_BUFFER_SIZE];
    File[] testFiles = new File[NUM_TEST_FILES];
    for (int i = 0; i < NUM_TEST_FILES; ++i) {
      File testFile = new File(workDirectoryFile, "hank_random_read_performance_file_" + i);
      testFiles[i] = testFile;
      if (!testFile.createNewFile()) {
        throw new IOException("Failed to create new file: " + testFile.getAbsolutePath());
      }
      LOG.info("Creating file " + testFile.getAbsolutePath() + " with random bytes (" +
          UiUtils.formatNumBytes(NUM_TEST_FILE_RANDOM_BYTES_BUFFERS * RANDOM_BYTES_BUFFER_SIZE) + ")");
      OutputStream testFileOutputStream = new BufferedOutputStream(new FileOutputStream(testFile));
      // Write random bytes
      for (int j = 0; j < NUM_TEST_FILE_RANDOM_BYTES_BUFFERS; ++j) {
        random.nextBytes(randomBytes);
        testFileOutputStream.write(randomBytes);
      }
      testFileOutputStream.close();
    }
    return testFiles;
  }

  private static class RandomReadsRunnable implements Runnable {

    private final FileChannel[] testChannels;
    private final HankTimerAggregator timerAggregator;

    public RandomReadsRunnable(File[] testFiles) throws FileNotFoundException {
      // Open file channels
      testChannels = new FileChannel[NUM_TEST_FILES];
      for (int i = 0; i < NUM_TEST_FILES; ++i) {
        testChannels[i] = new FileInputStream(testFiles[i]).getChannel();
      }
      timerAggregator = new HankTimerAggregator("Random reads", GET_TIMER_AGGREGATOR_WINDOW);
    }

    @Override
    public void run() {
      try {
        Random random = new Random();
        // Perform random reads
        byte[] readBufferArray = new byte[RANDOM_BYTES_BUFFER_SIZE];
        ByteBuffer readBuffer = ByteBuffer.wrap(readBufferArray);
        for (int i = 0; i < NUM_RANDOM_READS; ++i) {
          readBuffer.clear();
          HankTimer timer = new HankTimer();
          long randomPosition = Math.abs(random.nextLong()) % (NUM_TEST_FILE_RANDOM_BYTES_BUFFERS);
          testChannels[i % NUM_TEST_FILES]
              .position(randomPosition)
              .read(readBuffer);
          timerAggregator.add(timer);
        }
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
