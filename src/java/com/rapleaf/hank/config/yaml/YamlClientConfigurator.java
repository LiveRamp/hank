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
package com.rapleaf.hank.config.yaml;

import java.io.FileNotFoundException;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.config.InvalidConfigurationException;

public class YamlClientConfigurator extends BaseYamlConfigurator implements ClientConfigurator {
  public YamlClientConfigurator() {
    super();
  }

  public YamlClientConfigurator(String configPath) throws FileNotFoundException, InvalidConfigurationException {
    super(configPath);
  }
}
