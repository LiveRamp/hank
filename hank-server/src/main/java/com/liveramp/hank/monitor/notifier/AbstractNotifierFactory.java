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

package com.liveramp.hank.monitor.notifier;

import com.liveramp.hank.config.InvalidConfigurationException;

import java.util.Map;

public abstract class AbstractNotifierFactory implements NotifierFactory {

  protected Object getRequiredConfigurationItem(Map<String, Object> configuration,
                                                String key) throws InvalidConfigurationException {
    if (!configuration.containsKey(key)) {
      throw new InvalidConfigurationException(
          "Required notifier factory configuration item '" + key + "' was not found.");
    }
    return configuration.get(key);
  }

  protected String getRequiredString(Map<String, Object> configuration,
                                     String key) throws InvalidConfigurationException {
    return (String) getRequiredConfigurationItem(configuration, key);
  }

  protected String getString(Map<String, Object> configuration, String key) {
    try {
      return getRequiredString(configuration, key);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
  }
}
