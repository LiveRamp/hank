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
package com.rapleaf.hank.part_daemon;

import junit.framework.TestCase;

public class TestServer extends TestCase {
  public void testIt() {
    fail();
  }
 // public class MockCoordinator implements Coordinator {
 //    PartDaemonState partDaemonState = null;
 //    private StartableCallback startableCallback;
 //    private StoppableCallback stoppableCallback;
 // 
 //    @Override
 //    public void awaitStartable(String hostname, StartableCallback callback) {
 //      this.startableCallback = callback;
 //    }
 // 
 //    @Override
 //    public void awaitStoppable(String hostname, StoppableCallback callback) {
 //      this.stoppableCallback = callback;
 //    }
 // 
 //    @Override
 //    public void awaitStopped(String hostname, Object callback) {}
 // 
 //    @Override
 //    public void awaitUpdatable(String hostName, UpdatableCallback callback) {}
 // 
 //    @Override
 //    public void awaitUpdated(String hostname, Object callback) {}
 // 
 //    @Override
 //    public PartDaemonState getPartDaemonState(String hostname) {
 //      return partDaemonState;
 //    }
 // 
 //    @Override
 //    public void setServing(String hostname) {
 //      partDaemonState = PartDaemonState.SERVING;
 //    }
 // 
 //    @Override
 //    public void setStartable(String hostname) {
 //      partDaemonState = PartDaemonState.STARTABLE;
 //      startableCallback.notifyStartable();
 //    }
 // 
 //    @Override
 //    public void setStarting(String hostname) {
 //      partDaemonState = PartDaemonState.STARTING;
 //    }
 // 
 //    @Override
 //    public void setStoppable(String hostname) {
 //      partDaemonState = PartDaemonState.STOPPABLE;
 //      stoppableCallback.notifyStoppable();
 //    }
 // 
 //    @Override
 //    public void setStopped(String hostname) {
 //      partDaemonState = PartDaemonState.STOPPED;
 //    }
 // 
 //    @Override
 //    public void setStopping(String hostname) {
 //      partDaemonState = PartDaemonState.STOPPING;
 //    }
 // 
 //    @Override
 //    public void setUpdateable(String hostname) {}
 // 
 //    @Override
 //    public void setUpdated(String hostname) {}
 // 
 //    @Override
 //    public void setUpdating(String hostname) {}
 // 
 //    @Override
 //    public List<String> getHostsForPartition(int partition) {
 //      // TODO Auto-generated method stub
 //      return null;
 //    }
 //  }
 // 
 //  public class MockPartDaemonConfigurator implements PartDaemonConfigurator {
 //    private final MockCoordinator mockCoordinator;
 // 
 //    public MockPartDaemonConfigurator() {
 //      mockCoordinator = new MockCoordinator();
 //    }
 // 
 //    @Override
 //    public int getNumThreads() {
 //      return 1;
 //    }
 // 
 //    @Override
 //    public int getServicePort() {
 //      return 12345;
 //    }
 // 
 //    @Override
 //    public Set<String> getLocalDataDirectories() {
 //      // TODO Auto-generated method stub
 //      return null;
 //    }
 // 
 //    @Override
 //    public Coordinator getCoordinator() {
 //      return mockCoordinator;
 //    }
 //  }
 // 
 //  public void testMovesThroughStates() throws Exception {
 //    MockPartDaemonConfigurator conf = new MockPartDaemonConfigurator();
 //    final Server server = new Server(conf);
 // 
 //    Runnable serverRunnable = new Runnable() {
 //      @Override
 //      public void run() {
 //        server.run();
 //      }
 //    };
 // 
 //    Thread serverThread = new Thread(serverRunnable);
 //    serverThread.start();
 // 
 //    conf.getCoordinator().setStartable(null);
 // 
 //    // check that coordinator was notified of the server serving
 //    assertEquals(Coordinator.PartDaemonState.SERVING, conf.getCoordinator().getPartDaemonState(null));
 //    // verify through thrift interface that server is up
 // 
 //    // set server to stoppable
 //    conf.getCoordinator().setStoppable(null);
 // 
 //    // check that server sets itself to stopped
 //    assertEquals(Coordinator.PartDaemonState.STOPPED, conf.getCoordinator().getPartDaemonState(null));
 //    // verify that the thrift interface is down
 // 
 //    // set server to startable
 //    conf.getCoordinator().setStartable(null);
 // 
 //    // check that the server is up again
 //    assertEquals(Coordinator.PartDaemonState.SERVING, conf.getCoordinator().getPartDaemonState(null));
 // 
 //    // actually down the server
 //    server.stop();
 //    serverThread.join();
 //  }
}
