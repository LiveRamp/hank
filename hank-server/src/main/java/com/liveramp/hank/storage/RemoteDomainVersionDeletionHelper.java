/**
 *  Copyright 2014 LiveRamp
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

package com.liveramp.hank.storage;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlCoordinatorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.util.CommandLineChecker;

public class RemoteDomainVersionDeletionHelper {

  private static Logger LOG = Logger.getLogger(RemoteDomainVersionDeletionHelper.class);

  public static void main(String[] args) throws IOException, InvalidConfigurationException {
    CommandLineChecker.check(args, new String[]{"configuration", "domain name", "domain version number"},
        RemoteDomainVersionDeletionHelper.class
    );

    String configurationPath = args[0];
    String domainName = args[1];
    Integer versionNumber = Integer.parseInt(args[2]);

    Coordinator coordinator = new YamlCoordinatorConfigurator(configurationPath).createCoordinator();
    Domain domain = coordinator.getDomain(domainName);
    if (domain == null) {
      throw new RuntimeException("Given domain was not found: " + domainName);
    }

    LOG.info("Deleting remote data for domain " + domainName + " version " + versionNumber);
    domain.getStorageEngine().getRemoteDomainVersionDeleter().deleteVersion(versionNumber);
  }
}
