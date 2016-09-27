/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.hank.storage.incremental;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlConfigurator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.DomainVersionProperties;
import com.liveramp.hank.coordinator.DomainVersionPropertiesSerialization;
import com.liveramp.hank.coordinator.Domains;

public class IncrementalDomainVersionProperties implements DomainVersionProperties {

  private final Integer parentVersion;
  private final String source;

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
    this.parentVersion = parentVersion;
    this.source = source;
  }

  public Integer getParentVersionNumber() {
    return parentVersion;
  }

  public String getSource() {
    return source;
  }

  public boolean isBase() {
    return getParentVersionNumber() == null;
  }

  public static boolean isBase(DomainVersion domainVersion) throws IOException {
    IncrementalDomainVersionProperties properties = (IncrementalDomainVersionProperties)domainVersion.getProperties();
    if (properties == null) {
      throw new RuntimeException("Given Domain Version properties are null.");
    }
    return properties.isBase();
  }

  public static DomainVersion getParentDomainVersion(Domain domain, DomainVersion version) throws IOException {
    IncrementalDomainVersionProperties properties = (IncrementalDomainVersionProperties)version.getProperties();
    if (properties == null) {
      throw new IOException("Failed to get parent of Domain Version since corresponding properties are empty: " + version);
    } else {
      Integer parentVersionNumber = properties.getParentVersionNumber();
      if (parentVersionNumber == null) {
        return null;
      } else {
        DomainVersion result = domain.getVersion(parentVersionNumber);
        if (result == null) {
          throw new IOException("Failed to get parent Domain Version since specified parent version number ("
              + parentVersionNumber + ") of version " + version.getVersionNumber() + " of Domain " + domain.getName() + " does not correspond to any version.");
        }
        return result;
      }
    }
  }

  public static class Serialization implements DomainVersionPropertiesSerialization {

    private static final String PARENT_KEY = "parent";
    private static final String SOURCE_KEY = "source";
    private static final String SERIALIZATION_CHARSET = "UTF-8";

    private static class Configurator extends YamlConfigurator {

      @Override
      protected void validate() throws InvalidConfigurationException {
        this.checkNonEmptyConfiguration();
        this.getRequiredInteger(PARENT_KEY);
      }

      protected Integer getParentVersionNumber() throws InvalidConfigurationException {
        return getRequiredInteger(PARENT_KEY);
      }

      protected String getSource() {
        return getOptionalString(SOURCE_KEY);
      }
    }

    @Override
    public DomainVersionProperties deserializeProperties(byte[] serializedProperties) throws IOException {
      String yaml;
      try {
        yaml = new String(serializedProperties, SERIALIZATION_CHARSET);
      } catch (UnsupportedEncodingException e) {
        throw new IOException("Failed to deserialize domain version properties.", e);
      }
      Configurator configurator = new Configurator();
      try {
        configurator.loadFromYaml(yaml);
        return new IncrementalDomainVersionProperties(
            configurator.getParentVersionNumber(),
            configurator.getSource());
      } catch (InvalidConfigurationException e) {
        throw new IOException("Failed to deserialize domain version properties.", e);
      }
    }

    @Override
    public byte[] serializeProperties(DomainVersionProperties propertiesObj) throws IOException {
      IncrementalDomainVersionProperties properties = (IncrementalDomainVersionProperties)propertiesObj;
      Configurator configurator = new Configurator();
      Map<String, Object> yaml = new TreeMap<String, Object>();
      yaml.put(PARENT_KEY, properties.getParentVersionNumber());
      if (properties.getSource() != null) {
        yaml.put(SOURCE_KEY, properties.getSource());
      }
      try {
        configurator.loadFromObjectMap(yaml);
      } catch (InvalidConfigurationException e) {
        throw new IOException("Failed to serialize domain version properties.", e);
      }
      return configurator.toYaml().getBytes(SERIALIZATION_CHARSET);
    }
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    Integer parentVersionNumber = getParentVersionNumber();
    String source = getSource();
    if (parentVersionNumber == null) {
      result.append("Base");
    } else {
      result.append("Delta (parent: " + parentVersionNumber + ")");
    }
    if (source != null) {
      result.append(", Source: ");
      result.append(source);
    }
    return result.toString();
  }
}
