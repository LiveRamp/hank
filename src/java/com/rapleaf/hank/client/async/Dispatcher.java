/*
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

package com.rapleaf.hank.client.async;

import com.rapleaf.hank.client.GetCallback;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartitionServer;
import com.rapleaf.hank.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Dispatcher implements Runnable {

  private static final Logger LOG = Logger.getLogger(Dispatcher.class);
  private static final HankResponse NO_CONNECTION_AVAILABLE_RESPONSE
      = HankResponse.xception(HankException.no_connection_available(true));
  private static final HankBulkResponse NO_CONNECTION_AVAILABLE_BULK_RESPONSE
      = HankBulkResponse.xception(HankException.no_connection_available(true));
  private static final HankResponse TIMEOUT_RESPONSE // TODO: Add new error type for queryTimeoutNano
      = HankResponse.xception(HankException.internal_error("Request queryTimeoutNano"));

  private final BlockingQueue<GetTask> getTasks;
  private final long queryTimeoutNano;
  private final long bulkQueryTimeoutNano;
  private final int queryMaxNumTries;
  private Thread dispatcherThread;
  private volatile boolean stopping = false;

  public Dispatcher(int queryTimeoutMs, int bulkQueryTimeoutMs, int queryMaxNumTries) {
    // Initialize select queues
    getTasks = new LinkedBlockingQueue<GetTask> ();
    this.queryTimeoutNano = queryTimeoutMs * 1000000; // convert ms to nano
    this.bulkQueryTimeoutNano = bulkQueryTimeoutMs * 1000000; // convert ms to nano
    this.queryMaxNumTries = queryMaxNumTries;
  }

  public Runnable getOnChangeRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        //dispatcherThread.interrupt();
      }
    };
  }

  protected class GetTask {

    private final int domainId;
    private final ByteBuffer key;
    private final HostConnectionPool hostConnectionPool;
    private final GetCallback resultHandler;
    private int tryCount;
    private HostConnectionPool.HostConnectionAndHostIndex hostConnectionAndHostIndex;
    private HankResponse response;
    private Long startNanoTime;

    public GetTask(int domainId,
                   ByteBuffer key,
                   HostConnectionPool hostConnectionPool,
                   GetCallback resultHandler) {
      this.domainId = domainId;
      this.key = key;
      this.hostConnectionPool = hostConnectionPool;
      this.resultHandler = resultHandler;
      this.tryCount = 0;
      this.hostConnectionAndHostIndex = null;
      this.startNanoTime = null;
    }

    public void disconnect() {
      if (hostConnectionAndHostIndex != null && hostConnectionAndHostIndex.hostConnection != null) {
        hostConnectionAndHostIndex.hostConnection.attemptDisconnect();
      }
    }

    public void releaseConnection() {
      if (hostConnectionAndHostIndex != null && hostConnectionAndHostIndex.hostConnection != null) {
        hostConnectionAndHostIndex.hostConnection.setIsBusy(false);
      }
    }

    public void execute() {
      if (hasTimedout()) {
        // If we timedout just complete the task with timeout response
        response = TIMEOUT_RESPONSE;
        doCompleted();
      } else {
        if (hostConnectionAndHostIndex == null) {
          hostConnectionAndHostIndex = hostConnectionPool.findConnectionToUse();
        } else {
          hostConnectionAndHostIndex = hostConnectionPool.findConnectionToUse(hostConnectionAndHostIndex.hostIndex);
        }

        if (hostConnectionAndHostIndex == null) {
          // All connections are busy, add it back to the dispatcher queue
          addTask(this);
        } else if (hostConnectionAndHostIndex.hostConnection == null) {
          // All hosts were in standby, set the response appropriately and complete task
          response = NO_CONNECTION_AVAILABLE_RESPONSE;
          doCompleted();
        } else {
          // Claim connection
          hostConnectionAndHostIndex.hostConnection.setIsBusy(true);
          // Execute asynchronous task
          hostConnectionAndHostIndex.hostConnection.get(domainId, key, new GetTask.Callback());
        }
      }
    }

    private void doCompleted() {
      resultHandler.onComplete(response);
    }

    private void transition() {
      ++tryCount;
      if (response.is_set_xception() && tryCount < queryMaxNumTries) {
        // Error: add it back to the dispatcher queue
        addTask(this);
      } else {
        // Success
        doCompleted();
      }
    }

    private long getNanoTimeBeforeTimeout(long currentNanoTime) {
      // Return time in nanosecond before task queryTimeoutNano. Return 0 if task has timed out.
      return Math.max(queryTimeoutNano - Math.abs(currentNanoTime - startNanoTime), 0);
    }

    private boolean hasTimedout() {
      if (queryTimeoutNano != 0) {
        long currentNanoTime = System.nanoTime();
        long taskNanoTimeBeforeTimeout = getNanoTimeBeforeTimeout(currentNanoTime);
        if (taskNanoTimeBeforeTimeout == 0) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      return "GetTask [domainId=" + domainId + ", key=" + Bytes.bytesToHexString(key) + ", tryCount=" + tryCount + "]";
    }

    private class Callback implements HostConnectionGetCallback {

      @Override
      public void onComplete(PartitionServer.AsyncClient.get_call response) {
        try {
          GetTask.this.response = response.getResult();
        } catch (TException e) {
          // Always disconnect in case of Thrift error
          disconnect();
          String errMsg = "Failed to load GET result: " + e.getMessage();
          LOG.error(errMsg);
          GetTask.this.response = HankResponse.xception(HankException.internal_error(errMsg));
        } finally {
          // Always release the connection and transition
          releaseConnection();
          GetTask.this.transition();
        }
      }

      @Override
      public void onError(Exception e) {
        // Always disconnect in case of Thrift error
        disconnect();
        try {
          String errMsg = "Failed to execute GET: " + e.getMessage();
          LOG.error(errMsg);
          GetTask.this.response = HankResponse.xception(HankException.internal_error(errMsg));
        } finally {
          // Always release the connection and transition
          releaseConnection();
          GetTask.this.transition();
        }
      }
    }
  }

  public void stop() {
    stopping = true;
    dispatcherThread.interrupt();
  }

  public void setDispatcherThread(DispatcherThread dispatcherThread) {
    if (this.dispatcherThread != null) {
      throw new RuntimeException("Tried to set dispatcher thread but it was already set.");
    }
    this.dispatcherThread = dispatcherThread;
  }

  public void addTask(GetTask task) {
    try {
      // TODO: remove trace
      //LOG.trace("Adding task with state " + task.state);
      if (task.startNanoTime == null) {
        task.startNanoTime = System.nanoTime();
      }
      getTasks.put(task);
      //LOG.trace("Get Task is now " + getTasks.size());
    } catch (InterruptedException e) {
      // Someone is trying to stop Dispatcher
    }
  }

  @Override
  public void run() {
    while (!stopping) {
      try {
        GetTask task = getTasks.take();
        // TODO: remove trace
        //LOG.trace("Acquire task with state " + task.state);
        task.execute();
      } catch (InterruptedException e) {
        // Someone is trying to stop Dispatcher
      }
    }
  }
}
