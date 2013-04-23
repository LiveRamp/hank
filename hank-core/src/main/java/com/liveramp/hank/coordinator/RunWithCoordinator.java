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

package com.liveramp.hank.coordinator;

import com.liveramp.hank.config.CoordinatorConfigurator;

import java.io.IOException;

public class RunWithCoordinator {

  public static Coordinator createCoordinator(CoordinatorConfigurator configurator) {
    if (configurator == null) {
      throw new RuntimeException("Provided Hank Configurator is null. Cannot create Coordinator.");
    }
    Coordinator coordinator = configurator.createCoordinator();
    if (coordinator == null) {
      throw new RuntimeException("A null Coordinator was returned by the provided Configurator: " + configurator);
    }
    return coordinator;
  }

  public static void run(CoordinatorConfigurator configurator, RunnableWithCoordinator runnableWithCoordinator) throws IOException {
    Coordinator coordinator = createCoordinator(configurator);
    try {
      runnableWithCoordinator.run(coordinator);
    } finally {
      coordinator.close();
    }
  }
}
