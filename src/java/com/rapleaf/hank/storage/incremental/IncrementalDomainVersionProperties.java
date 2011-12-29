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
import com.rapleaf.hank.config.yaml.YamlConfigurator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.DomainVersionProperties;
import com.rapleaf.hank.coordinator.Domains;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class IncrementalDomainVersionProperties extends YamlConfigurator implements DomainVersionProperties {

  private static final String PARENT_KEY = "parent";
  private static final String SOURCE_KEY = "source";

  // Static helper classes to create properties objects

  public static class Base extends IncrementalDomainVersionProperties {

    public Base() {
      super(null);
    }

    public Base(String source) {
      super(null, source);
    }
  }

  public static class Delta extends IncrementalDomainVersionProperties {

    public Delta(int parentVersion) {
      this(parentVersion, null);
    }

    public Delta(int parentVersion, String source) {
      super(parentVersion, source);
    }

    public Delta(Domain domain) throws IOException {
      this(domain, null);
    }

    public Delta(Domain domain, String source) throws IOException {
      this(Domains.getLatestVersion(domain), source);
    }

    public Delta(DomainVersion parentVersion) {
      this(parentVersion, null);
    }

    public Delta(DomainVersion parentVersion, String source) {
      super(parentVersion == null ? null : parentVersion.getVersionNumber(), source);
    }
  }

  public IncrementalDomainVersionProperties(Integer parentVersion) {
    this(parentVersion, null);
  }

  public IncrementalDomainVersionProperties(Integer parentVersion, String source) {
    try {
      Map<String, Object> yaml = new TreeMap<String, Object>();
      yaml.put(PARENT_KEY, parentVersion);
      if (source != null) {
        yaml.put(SOURCE_KEY, source);
      }
      loadFromObjectMap(yaml);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException("Failed to construct IncrementalDomainVersionProperties.", e);
    }
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    this.checkNonEmptyConfiguration();
    this.getRequiredInteger(PARENT_KEY);
  }

  public Integer getParentVersionNumber() {
    return getInteger(PARENT_KEY);
  }

  public boolean isBase() {
    return getParentVersionNumber() == null;
  }

  public static DomainVersion getParentDomainVersion(Domain domain, DomainVersion version) throws IOException {
    IncrementalDomainVersionProperties properties = (IncrementalDomainVersionProperties) version.getProperties();
    if (properties == null) {
      throw new IOException("Failed to get parent of Domain Version since corresponding properties are empty." + version);
    } else {
      Integer parentVersionNumber = properties.getParentVersionNumber();
      if (parentVersionNumber == null) {
        return null;
      } else {
        return domain.getVersionByNumber(parentVersionNumber);
      }
    }
  }
}
