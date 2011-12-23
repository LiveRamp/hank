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

package com.rapleaf.hank.client;

import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartitionServer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;

class SelectRunnable implements Runnable {

  private static final Logger LOG = Logger.getLogger(SelectRunnable.class);

  private final LinkedList<GetTask> getTasks;
  private final LinkedList<GetTask> getTasksComplete;
  private final Semaphore workSemaphore = new Semaphore(0, true);

  protected class GetTask {

    private final int domainId;
    private final ByteBuffer key;
    private final AsyncHostConnectionPool hostConnectionPool;
    private final GetCallback resultHanlder;
    private int retry;
    private AsyncHostConnectionPool.AsyncHostConnectionAndHostIndex hostConnectionAndHostIndex;
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
                   AsyncHostConnectionPool hostConnectionPool,
                   GetCallback resultHandler) {
      this.domainId = domainId;
      this.key = key;
      this.hostConnectionPool = hostConnectionPool;
      this.resultHanlder = resultHandler;
      this.retry = 0;
      this.hostConnectionAndHostIndex = null;
    }

    public void complete() {
      resultHanlder.onComplete(response);
    }

    public void releaseConnection() {
      hostConnectionAndHostIndex.hostConnection.setIsBusy(false);
    }

    public boolean execute() {
      if (hostConnectionAndHostIndex == null) {
        hostConnectionAndHostIndex = hostConnectionPool.findConnectionToUse();
      } else {
        hostConnectionAndHostIndex = hostConnectionPool.findConnectionToUse(hostConnectionAndHostIndex.hostIndex);
      }
      if (hostConnectionAndHostIndex == null) {
        return false;
      }
      // Claim connection
      hostConnectionAndHostIndex.hostConnection.setIsBusy(true);
      // Execute asynchronous task
      hostConnectionAndHostIndex.hostConnection.get(domainId, key, new Callback());
      return true;
    }
  }

  public SelectRunnable() {
    // Initialize select queues
    getTasks = new LinkedList<GetTask>();
    getTasksComplete = new LinkedList<GetTask>();
  }

  public void addTask(GetTask task) {
    synchronized (getTasks) {
      getTasks.addLast(task);
    }
    workSemaphore.release();
  }

  public void addCompleteTask(GetTask task) {
    synchronized (getTasksComplete) {
      getTasksComplete.addLast(task);
    }
    workSemaphore.release();
  }

  @Override
  public void run() {
    while (true) {
      completeTasks();
      executeTasks();
      try {
        workSemaphore.acquire(1);
      } catch (InterruptedException e) {
        break;
      }
      workSemaphore.drainPermits();
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

  private void executeTasks() {
    synchronized (getTasks) {
      Iterator<GetTask> iterator = getTasks.iterator();
      while (iterator.hasNext()) {
        GetTask task = iterator.next();
        if (task.execute()) {
          getTasks.remove(iterator);
        }
      }
    }
  }
}
