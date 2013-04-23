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

package com.liveramp.hank.config;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SimpleDataDirectoriesConfigurator implements DataDirectoriesConfigurator {

  private final Set<String> dataDirectories;

  public SimpleDataDirectoriesConfigurator(String dataDirectory) {
    this.dataDirectories = Collections.singleton(dataDirectory);
  }

  public SimpleDataDirectoriesConfigurator(Collection<String> dataDirectories) {
    this.dataDirectories = new HashSet<String>();
    this.dataDirectories.addAll(dataDirectories);
  }

  @Override
  public Set<String> getDataDirectories() {
    return dataDirectories;
  }
}
