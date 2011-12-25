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
import java.util.Iterator;
import java.util.LinkedList;

public class Dispatcher implements Runnable {

  private static final Logger LOG = Logger.getLogger(Dispatcher.class);
  private static final HankResponse NO_CONNECTION_AVAILABLE_RESPONSE
      = HankResponse.xception(HankException.no_connection_available(true));
  private static final HankBulkResponse NO_CONNECTION_AVAILABLE_BULK_RESPONSE
      = HankBulkResponse.xception(HankException.no_connection_available(true));

  private final LinkedList<GetTask> getTasks;
  private final LinkedList<GetTask> getTasksComplete;
  private Thread dispatcherThread;
  private volatile boolean stopping = false;

  public Runnable getOnChangeRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        dispatcherThread.interrupt();
      }
    };
  }

  protected class GetTask {

    private final int domainId;
    private final ByteBuffer key;
    private final HostConnectionPool hostConnectionPool;
    private final GetCallback resultHanlder;
    private int retry;
    private HostConnectionPool.HostConnectionAndHostIndex hostConnectionAndHostIndex;
    private HankResponse response;

    private class Callback implements HostConnectionGetCallback {

      @Override
      public void onComplete(PartitionServer.AsyncClient.get_call response) {
        try {
          GetTask.this.response = response.getResult();
        } catch (TException e) {
          String errMsg = "Failed to load GET result: " + e.getMessage();
          LOG.error(errMsg);
          GetTask.this.response =
              HankResponse.xception(HankException.internal_error(errMsg));
        } finally {
          addCompleteTask(GetTask.this);
        }
      }

      @Override
      public void onError(Exception e) {
        try {
          String errMsg = "Failed to execute GET: " + e.getMessage();
          LOG.error(errMsg);
          GetTask.this.response =
              HankResponse.xception(HankException.internal_error(errMsg));
        } finally {
          addCompleteTask(GetTask.this);
        }
      }
    }

    public GetTask(int domainId,
                   ByteBuffer key,
                   HostConnectionPool hostConnectionPool,
                   GetCallback resultHandler) {
      this.domainId = domainId;
      this.key = key;
      this.hostConnectionPool = hostConnectionPool;
      this.resultHanlder = resultHandler;
      this.retry = 0;
      this.hostConnectionAndHostIndex = null;
    }

    public void complete() {
      LOG.trace("Completing task " + this);
      resultHanlder.onComplete(response);
    }

    public void releaseConnection() {
      if (hostConnectionAndHostIndex.hostConnection != null) {
        LOG.trace("Releasing connection for task " + this);
        hostConnectionAndHostIndex.hostConnection.setIsBusy(false);
      }
    }

    public boolean execute() {
      if (hostConnectionAndHostIndex == null) {
        hostConnectionAndHostIndex = hostConnectionPool.findConnectionToUse();
      } else {
        hostConnectionAndHostIndex = hostConnectionPool.findConnectionToUse(hostConnectionAndHostIndex.hostIndex);
      }

      if (hostConnectionAndHostIndex == null) {
        //TODO: remove trace
        LOG.trace("No connection found for task " + this);
        return false;
      }

      // All hosts were in standby, set the response appropriately and complete task
      if (hostConnectionAndHostIndex.hostConnection == null) {
        response = NO_CONNECTION_AVAILABLE_RESPONSE;
        addCompleteTask(this);
        return true;
      }

      // Claim connection
      hostConnectionAndHostIndex.hostConnection.setIsBusy(true);
      // Execute asynchronous task
      hostConnectionAndHostIndex.hostConnection.get(domainId, key, new GetTask.Callback());
      //TODO: remove trace
      LOG.trace("Executing task " + this);
      return true;
    }

    @Override
    public String toString() {
      return "GetTask [domainId=" + domainId + ", key=" + Bytes.bytesToHexString(key) + ", retry=" + retry + "]";
    }
  }

  public Dispatcher() {
    // Initialize select queues
    getTasks = new LinkedList<GetTask>();
    getTasksComplete = new LinkedList<GetTask>();
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
    synchronized (getTasks) {
      getTasks.addLast(task);
    }
    dispatcherThread.interrupt();
  }

  public void addCompleteTask(GetTask task) {
    synchronized (getTasksComplete) {
      getTasksComplete.addLast(task);
    }
    dispatcherThread.interrupt();
  }

  @Override
  public void run() {
    while (!stopping) {
      //TODO: remove trace
      LOG.trace("--------------------------");
      completeTasks();
      startTasks();
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
        // There is work to do
      }
    }
  }

  private void completeTasks() {
    synchronized (getTasksComplete) {
      for (GetTask task : getTasksComplete) {
        // Execute result handler
        task.complete();
        // Release connection
        task.releaseConnection();
      }
      getTasksComplete.clear();
    }
  }

  private void startTasks() {
    synchronized (getTasks) {
      Iterator<GetTask> iterator = getTasks.iterator();
      while (iterator.hasNext()) {
        GetTask task = iterator.next();
        if (task.execute()) {
          iterator.remove();
        }
      }
    }
  }
}
