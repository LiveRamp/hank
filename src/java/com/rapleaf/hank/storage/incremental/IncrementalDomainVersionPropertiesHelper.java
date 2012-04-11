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

package com.rapleaf.hank.storage.incremental;

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.util.CommandLineChecker;
import org.apache.log4j.Logger;

import java.io.IOException;

public class IncrementalDomainVersionPropertiesHelper {

  private static Logger LOG = Logger.getLogger(IncrementalDomainVersionPropertiesHelper.class);

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

    Coordinator coordinator = new YamlClientConfigurator(configurationPath).createCoordinator();
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
