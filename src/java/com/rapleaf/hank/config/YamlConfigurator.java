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
package com.rapleaf.hank.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;

public class YamlConfigurator implements PartDaemonConfigurator, UpdateDaemonConfigurator {
  @SuppressWarnings("unused")
  private static final Logger LOG = Logger.getLogger(YamlConfigurator.class);

  public static final String KEY_LOCAL_DATA_DIRS = "local_data_dirs";
  public static final String KEY_RING_GROUP_NAME = "ring_group_name";
  public static final String KEY_RING_NUMBER = "ring_number";

  public static final String KEY_PART_DAEMON = "part_daemon";
  public static final String KEY_NUM_THREADS = "num_threads";
  public static final String KEY_SERVICE_PORT = "service_port";

  public static final String KEY_UPDATE_DAEMON = "update_daemon";
  public static final String KEY_NUM_CONCURRENT_UPDATES = "num_concurrent_updates";

  public static final String KEY_COORDINATOR = "coordinator";
  public static final String KEY_FACTORY = "factory";
  public static final String KEY_OPTIONS = "options";

  private String configPath;
  private Object config;

  public YamlConfigurator(String configPath) throws IOException {
    this.configPath = configPath;
    Yaml yaml = new Yaml();
    InputStream input = new FileInputStream(new File(configPath));
    config = yaml.load(input);
  }

  public void loadConfig() throws FileNotFoundException {
    Yaml yaml = new Yaml();
    InputStream input = new FileInputStream(new File(configPath));
    config = yaml.load(input);
  }

  public void setConfigPath(String configPath) {
    this.configPath = configPath;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int getNumThreads() {
    return ((Map<String, Map<String, Integer>>)config).get(KEY_PART_DAEMON).get(KEY_NUM_THREADS);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int getServicePort() {
    return ((Map<String, Map<String, Integer>>)config).get(KEY_PART_DAEMON).get(KEY_SERVICE_PORT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Set<String> getLocalDataDirectories() {
    ArrayList<String> dirList = ((Map<String, ArrayList<String>>)config)
        .get(KEY_LOCAL_DATA_DIRS);
    Set<String> dirSet = new HashSet<String>();
    for (String dir : dirList) {
      dirSet.add(dir);
    }
    return dirSet;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Coordinator getCoordinator() {
    CoordinatorFactory factory;
    try {
      factory = (CoordinatorFactory) Class.forName(
          ((Map<String, Map<String, String>>)config).get(KEY_COORDINATOR).get(KEY_FACTORY)).newInstance();
    }
    catch (InstantiationException e) { throw new RuntimeException(e); } 
    catch (IllegalAccessException e) { throw new RuntimeException(e); }
    catch (ClassNotFoundException e) { throw new RuntimeException(e); }
    Map<String, String> options = ((Map<String, Map<String, Map<String, String>>>)config).get(KEY_COORDINATOR).get(KEY_OPTIONS);
    return factory.getCoordinator(options);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int getNumConcurrentUpdates() {
    return ((Map<String, Map<String, Integer>>)config).get(KEY_UPDATE_DAEMON).get(KEY_NUM_CONCURRENT_UPDATES);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String getRingGroupName() {
    return ((Map<String, String>)config).get(KEY_RING_GROUP_NAME).toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public int getRingNumber() {
    return ((Map<String, Integer>)config).get(KEY_RING_NUMBER);
  }
}
