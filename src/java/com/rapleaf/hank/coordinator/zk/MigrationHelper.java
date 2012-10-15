/**
 *  Copyright 2012 Rapleaf
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

package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.util.CommandLineChecker;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class MigrationHelper {

  private static Logger LOG = Logger.getLogger(MigrationHelper.class);

  public static void main(String[] args) throws IOException, InvalidConfigurationException, InterruptedException, KeeperException {
    CommandLineChecker.check(args, new String[]{"configuration"},
        MigrationHelper.class);

    String configurationPath = args[0];

    Coordinator coordinator = new YamlClientConfigurator(configurationPath).createCoordinator();

    for (DomainGroup domainGroup : coordinator.getDomainGroups()) {
      LOG.info("Migrating domain group " + domainGroup.getName());
      ZkDomainGroup zkDomainGroup = (ZkDomainGroup) domainGroup;
      NewZkDomainGroup result = zkDomainGroup.migrate();
      LOG.info(result);
    }
  }
}
