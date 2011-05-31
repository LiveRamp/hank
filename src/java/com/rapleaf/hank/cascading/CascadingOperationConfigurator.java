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

package com.rapleaf.hank.cascading;

import cascading.flow.FlowProcess;
import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.hadoop.DomainBuilderConfigurator;
import com.rapleaf.hank.hadoop.DomainBuilderDefaultOutputFormat;

public class CascadingOperationConfigurator extends DomainBuilderConfigurator implements Configurator {

  public CascadingOperationConfigurator(String domainName, FlowProcess flowProcess) {
    // Get configuration from FlowProcess
    super(getRequiredConfigurationItem(DomainBuilderDefaultOutputFormat.createConfParamName(domainName,
        DomainBuilderDefaultOutputFormat.CONF_PARAM_HANK_COORDINATOR_CONFIGURATION),
        "Hank coordinator configuration",
        flowProcess));
  }

  public static String getRequiredConfigurationItem(String key, String prettyName, FlowProcess flowProcess) {
    String result = (String) flowProcess.getProperty(key);
    if (result == null) {
      throw new RuntimeException(prettyName + " must be set with configuration item: " + key);
    }
    return result;
  }
}
