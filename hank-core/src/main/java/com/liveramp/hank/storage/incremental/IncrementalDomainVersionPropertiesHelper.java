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

package com.liveramp.hank.storage.incremental;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlCoordinatorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.util.CommandLineChecker;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IncrementalDomainVersionPropertiesHelper {

  private static Logger LOG = LoggerFactory.getLogger(IncrementalDomainVersionPropertiesHelper.class);

  public static void main(String[] args) throws IOException, InvalidConfigurationException {
    CommandLineChecker.check(args, new String[]{"configuration", "domain name", "domain version number",
        "new parent domain version number | 'null'", "new domain version source | 'null'"},
        IncrementalDomainVersionPropertiesHelper.class);

    String configurationPath = args[0];
    String domainName = args[1];
    Integer versionNumber = Integer.parseInt(args[2]);

    String parentVersionStr = args[3];
    Integer parentVersion;
    if (parentVersionStr.equals("null")) {
      parentVersion = null;
    } else {
      parentVersion = Integer.parseInt(parentVersionStr);
    }

    String source = args[4];
    if (source.equals("null")) {
      source = null;
    }

    IncrementalDomainVersionProperties properties = new IncrementalDomainVersionProperties(parentVersion, source);

    Coordinator coordinator = new YamlCoordinatorConfigurator(configurationPath).createCoordinator();
    Domain domain = coordinator.getDomain(domainName);
    if (domain == null) {
      throw new RuntimeException("Given domain was not found: " + domainName);
    }
    DomainVersion domainVersion = domain.getVersion(versionNumber);
    if (domainVersion == null) {
      throw new RuntimeException("Given version was not found: " + domainName + " version " + versionNumber);
    }
    LOG.info("Setting properties of domain " + domainName + " version " + versionNumber + " to: " + properties);
    domainVersion.setProperties(properties);
  }
}
