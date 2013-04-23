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
package com.rapleaf.hank.test;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.util.Condition;
import com.rapleaf.hank.util.FsUtils;
import com.rapleaf.hank.util.WaitUntil;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public abstract class BaseTestCase extends TestCase {
  static {
    Logger.getRootLogger().setLevel(Level.ALL);
  }

  protected final String localTmpDir = System.getProperty("tmpDir", "/tmp/hank_local_file_system");

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    FsUtils.rmrf(localTmpDir);
    new File(localTmpDir).mkdirs();
    System.err.println("------ test start ------");
    System.out.println("------ test start ------");
  }

  protected void waitUntilHost(final HostState state, final Host host) throws InterruptedException {
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return state.equals(host.getState());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  protected void waitUntilCommand(final HostCommand command, final Host host) throws InterruptedException {
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return command.equals(host.getCurrentCommand());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
