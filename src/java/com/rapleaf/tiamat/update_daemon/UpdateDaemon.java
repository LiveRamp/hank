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
//package com.rapleaf.tiamat.update_daemon;
//
//import java.io.IOException;
//import java.net.UnknownHostException;
//import java.util.Queue;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.ThreadFactory;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
//import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
//
//import com.rapleaf.tiamat.config.DomainConfig;
//import com.rapleaf.tiamat.config.RingConfig;
//import com.rapleaf.tiamat.config.UpdateDaemonConfigurator;
//import com.rapleaf.tiamat.coordinator.Coordinator;
//import com.rapleaf.tiamat.coordinator.Coordinator.DaemonState;
//import com.rapleaf.tiamat.coordinator.Coordinator.DaemonType;
//import com.rapleaf.tiamat.coordinator.Coordinator.StateChangeListener;
//import com.rapleaf.tiamat.storage.StorageEngine;
//import com.rapleaf.tiamat.util.HostUtils;
//import com.rapleaf.tiamat.util.ZooKeeperUtils;
//
//public class UpdateDaemon implements StateChangeListener {
//  private static final Logger LOG = Logger.getLogger(UpdateDaemon.class);
//
//  private static final String HOST_NAME;
//  static {
//    try {
//      HOST_NAME = HostUtils.getHostName();
//    } catch (UnknownHostException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  private final class UpdateToDo implements Runnable {
//    private final StorageEngine engine;
//    private final int partNum;
//    private final Queue<Throwable> exceptionQueue;
//
//    public UpdateToDo(StorageEngine engine, int partNum, Queue<Throwable> exceptionQueue) {
//      this.engine = engine;
//      this.partNum = partNum;
//      this.exceptionQueue = exceptionQueue;
//    }
//
//    @Override
//    public void run() {
//      try {
//        engine.getUpdater(configurator, partNum).update();
//      } catch (Throwable e) {
//        // TODO: i just *know* that i'm going to end up wishing i toStringed the
//        // storage engine and part num here
//        LOG.fatal("Failed to complete an UpdateToDo!", e);
//        exceptionQueue.add(e);
//      }
//    }
//  }
//
//  private final UpdateDaemonConfigurator configurator;
//  private final Coordinator coord;
//  private String hostPath;
//
//  private boolean goingDown;
//
//  public UpdateDaemon(UpdateDaemonConfigurator configurator) {
//    this.configurator = configurator;
//    hostPath = ZooKeeperUtils.getHostPath(configurator, HOST_NAME);
//    this.coord = configurator.getCoordinator();
//    coord.addStateChangeListener(hostPath, DaemonType.UPDATE_DAEMON, this);
//  }
//
//  private void run() throws UnknownHostException {
//    coord.setDaemonState(hostPath, DaemonType.UPDATE_DAEMON, DaemonState.IDLE);
//
//    goingDown = false;
//
//    while (!goingDown) {
//      try {
//        Thread.sleep(1000);
//      } catch (InterruptedException e) {
//        // TODO: probably being informed that we should shut down.
//        break;
//      }
//    }
//  }
//
//  private void update() {
//    ThreadFactory factory = new ThreadFactory() {
//      private int x = 0;
//
//      @Override
//      public Thread newThread(Runnable r) {
//        return new Thread(r, "Updater Thread Pool Thread #" + ++x);
//      }
//    };
//
//    ExecutorService executor = new ThreadPoolExecutor(
//        configurator.getNumConcurrentUpdates(),
//        configurator.getNumConcurrentUpdates(),
//        1, TimeUnit.DAYS,
//        new LinkedBlockingQueue<Runnable>(),
//        factory);
//    Queue<Throwable> exceptionQueue = new LinkedBlockingQueue<Throwable>();
//
//    RingConfig ringConfig = configurator.getRingGroupConfig().getRingConfigForHost(HOST_NAME);
//
//    for (DomainConfig domainConfig : configurator.getRingGroupConfig().getDomainGroupConfig().getDomains()) {
//      StorageEngine engine = domainConfig.getStorageEngine();
//
//      for (Integer part : ringConfig.getPartitionsForHost(domainConfig.getId(), HOST_NAME)) {
//        executor.execute(new UpdateToDo(engine, part, exceptionQueue));
//      }
//    }
//
//    try {
//      boolean terminated = false;
//      while (!terminated) {
//        terminated = executor.awaitTermination(30, TimeUnit.SECONDS);
//        // TODO: report on progress?
//        // TODO: check exception queue
//      }
//    } catch (InterruptedException e) {
//      // TODO: log and quit
//    }
//  }
//
//  public static void main(String[] args) throws IOException {
//    String configPath = args[0];
//    String log4jprops = args[1];
//
//    PropertyConfigurator.configure(log4jprops);
//
//    new UpdateDaemon(null).run();
//  }
//
//  @Override
//  public void onStateChange(String hostPath, DaemonType type, DaemonState state) {
//    if (state == DaemonState.UPDATEABLE) {
//      coord.setDaemonState(hostPath, DaemonType.UPDATE_DAEMON, DaemonState.UPDATING);
//      update();
//      coord.setDaemonState(hostPath, DaemonType.UPDATE_DAEMON, DaemonState.IDLE);
//    }
//  }
//}
