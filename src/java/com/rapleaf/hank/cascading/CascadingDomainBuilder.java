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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import com.rapleaf.hank.hadoop.DomainBuilderPropertiesConfigurator;

public class CascadingDomainBuilder {


  private final static Logger LOG = Logger.getLogger(CascadingDomainBuilder.class);

  private final Domain domain;
  private final Tap outputTap;
  private final DomainBuilderProperties properties;
  private Pipe pipe;
  private final String keyFieldName;
  private final String valueFieldName;
  private Integer version = null;

  public CascadingDomainBuilder(DomainBuilderProperties properties,
                                Pipe pipe,
                                String keyFieldName,
                                String valueFieldName) {
    // Load domain
    this.domain = DomainBuilderPropertiesConfigurator.getDomainConfig(properties);
    // Create Tap
    this.outputTap = new DomainBuilderTap(keyFieldName, valueFieldName, properties);
    this.properties = properties;
    this.pipe = pipe;
    this.keyFieldName = keyFieldName;
    this.valueFieldName = valueFieldName;
  }

  public void openNewVersion() throws IOException {
    version = domain.openNewVersion().getVersionNumber();
    if (version == null) {
      throw new IOException("Could not open a new version of domain " + properties.getDomainName());
    }
    LOG.info("Opened new version #" + version + " of domain " + properties.getDomainName());
  }

  public void cancelNewVersion() throws IOException {
    if (version != null) {
      LOG.info("Cancelling new version #" + version + " of domain " + properties.getDomainName());
      domain.getVersions().last().cancel();
      version = null;
    }
  }

  public void closeNewVersion() throws IOException {
    if (version != null) {
      LOG.info("Closing new version #" + version + " of domain " + properties.getDomainName());
      domain.getVersions().last().close();
      version = null;
    }
  }

  // Build a single domain
  public void build(Properties cascadingProperties,
                    Tap inputTap) throws IOException {

    pipe = new DomainBuilderAssembly(properties.getDomainName(), pipe, keyFieldName, valueFieldName);

    // Open new version and check for success
    Domain domainConfig = DomainBuilderPropertiesConfigurator.getDomainConfig(properties);
    openNewVersion();
    if (version == null) {
      throw new IOException("Could not open a new version of domain " + properties.getDomainName());
    }
    // Try to build new version
    try {
      new FlowConnector(properties.setCascadingProperties(cascadingProperties)).connect("HankCascadingDomainBuilder: " +
          properties.getDomainName() + " version " + version, inputTap, outputTap, pipe).complete();
    } catch (Exception e) {
      // In case of failure, cancel this new version
      domainConfig.getVersions().last().cancel();
      throw new IOException("Failed at building version " + version + " of domain " + properties.getDomainName() + ". Cancelling version.", e);
    }
    // Close the new version
    domainConfig.getVersions().last().close();
  }

  // Build multiple domains
  public static void buildDomains(Properties cascadingProperties,
                                  Map<String, Tap> sources,
                                  Map<String, Tap> otherSinks,
                                  Pipe[] otherTails,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {

    // Info output
    for (CascadingDomainBuilder domainBuilder : domainBuilders) {
      LOG.info("Building domain with " + domainBuilder.toString());
    }

    try {
      // Open new versions
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        domainBuilder.openNewVersion();
      }

      // Create tails
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        domainBuilder.pipe = new DomainBuilderAssembly(domainBuilder.properties.getDomainName(),
            domainBuilder.pipe,
            domainBuilder.keyFieldName,
            domainBuilder.valueFieldName);
      }

      // Update properties
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        domainBuilder.properties.setCascadingProperties(cascadingProperties);
      }

      // Construct tails array
      Pipe[] tails = new Pipe[domainBuilders.length + otherTails.length];
      // Copy tails from domain builders
      for (int i = 0; i < domainBuilders.length; ++i) {
        tails[i] = domainBuilders[i].pipe;
      }
      // Copy extra tails
      for (int i = 0; i < otherTails.length; ++i) {
        tails[i + domainBuilders.length] = otherTails[i];
      }

      // Construct sinks map
      Map<String, Tap> sinks = new HashMap<String, Tap>();
      // Add domain builder sinks
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        sinks.put(domainBuilder.properties.getDomainName(), domainBuilder.outputTap);
      }
      // Add extra sinks
      sinks.putAll(otherSinks);

      // Create job name String
      StringBuilder jobName = new StringBuilder("HankCascadingDomainBuilder ");
      for (int i = 0; i < domainBuilders.length; ++i) {
        if (i != 0) {
          jobName.append(", ");
        }
        CascadingDomainBuilder domainBuilder = domainBuilders[i];
        jobName.append(domainBuilder.domain.getName()).append(" version ").append(domainBuilder.version);
      }

      // Build new versions
      new FlowConnector(cascadingProperties)
          .connect(jobName.toString(), sources, sinks, tails).complete();

    } catch (Exception e) {
      // In case of failure, cancel new versions
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        domainBuilder.cancelNewVersion();
      }
      throw new IOException("Failed at building domains. Cancelling open versions.", e);
    }
    // Close new versions
    for (CascadingDomainBuilder domainBuilder : domainBuilders) {
      domainBuilder.closeNewVersion();
    }
  }

  // Build a single domain
  public void build(Map<Object, Object> cascadingProperties,
                    Tap inputTap) throws IOException {
    build(mapToProperties(cascadingProperties), inputTap);
  }

  // Build multiple domains
  public static void buildDomains(Properties cascadingProperties,
                                  Map<String, Tap> sources,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {
    buildDomains(cascadingProperties, sources, new HashMap<String, Tap>(), new Pipe[0], domainBuilders);
  }

  // Build multiple domains
  public static void buildDomains(Map<Object, Object> cascadingProperties,
                                  Map<String, Tap> sources,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {
    buildDomains(mapToProperties(cascadingProperties), sources, new HashMap<String, Tap>(), new Pipe[0], domainBuilders);
  }

  // Build multiple domains
  public static void buildDomains(Map<Object, Object> cascadingProperties,
                                  Map<String, Tap> sources,
                                  Map<String, Tap> otherSinks,
                                  Pipe[] otherTails,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {
    buildDomains(mapToProperties(cascadingProperties), sources, otherSinks, otherTails, domainBuilders);
  }

  public String toString() {
    return "CascadingDomainBuilder: Domain: " + properties.getDomainName() + ", Output Tap: " + outputTap;
  }

  private static Properties mapToProperties(Map<Object, Object> properties) {
    Properties newProperties = new Properties();
    newProperties.putAll(properties);
    return newProperties;
  }
}
