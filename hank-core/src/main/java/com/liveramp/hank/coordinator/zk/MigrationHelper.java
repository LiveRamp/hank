/**
 *  Copyright 2012 LiveRamp
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

package com.liveramp.hank.coordinator.zk;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlCoordinatorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.util.CommandLineChecker;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class MigrationHelper {

  private static Logger LOG = LoggerFactory.getLogger(MigrationHelper.class);

  public static void main(String[] args) throws IOException, InvalidConfigurationException, InterruptedException, KeeperException {
    CommandLineChecker.check(args, new String[]{"configuration"},
        MigrationHelper.class);

    String configurationPath = args[0];

    Coordinator coordinator = new YamlCoordinatorConfigurator(configurationPath).createCoordinator();
  }
}
