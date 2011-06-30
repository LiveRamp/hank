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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.hadoop.DomainBuilderOutputCommitter;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CascadingDomainBuilder {

  private final static Logger LOG = Logger.getLogger(CascadingDomainBuilder.class);

  private final Domain domain;
  private Tap outputTap = null;
  private final DomainBuilderProperties properties;
  private Pipe pipe;
  private final String keyFieldName;
  private final String valueFieldName;
  private DomainVersion domainVersion = null;

  public CascadingDomainBuilder(DomainBuilderProperties properties,
                                Pipe pipe,
                                String keyFieldName,
                                String valueFieldName) throws IOException {
    // Load domain
    this.domain = properties.getDomain();
    // Fail if unable to load domain
    if (domain == null) {
      throw new IOException("Could not load domain for: " + properties);
    }
    this.properties = properties;
    this.pipe = pipe;
    this.keyFieldName = keyFieldName;
    this.valueFieldName = valueFieldName;
  }

  public void openNewVersion() throws IOException {
    domainVersion = domain.openNewVersion();
    if (domainVersion == null) {
      throw new IOException("Could not open a new version of domain " +
          properties.getDomainName());
    }
    // Create Tap
    outputTap = new DomainBuilderTap(keyFieldName, valueFieldName,
        domainVersion.getVersionNumber(), properties);
    LOG.info("Opened new version #" + domainVersion.getVersionNumber() +
        " of domain " + properties.getDomainName());
  }

  public void cancelNewVersion() throws IOException {
    if (domainVersion != null) {
      LOG.info("Cancelling new version #" + domainVersion.getVersionNumber() +
          " of domain " + properties.getDomainName());
      domainVersion.cancel();
      domainVersion = null;
    }
  }

  public void closeNewVersion() throws IOException {
    if (domainVersion != null) {
      LOG.info("Closing new version #" + domainVersion.getVersionNumber() +
          " of domain " + properties.getDomainName());
      domainVersion.close();
      domainVersion = null;
    }
  }

  // Build a single domain using one source
  public Flow build(Properties cascadingProperties,
                    Tap source) throws IOException {
    return build(cascadingProperties, new TapOrTapMap(source));
  }

  // Build a single domain
  public Flow build(Properties cascadingProperties,
                    Map<String, Tap> sources) throws IOException {
    return build(cascadingProperties, new TapOrTapMap(sources));
  }

  // Build a single domain
  private Flow build(Properties cascasdingProperties,
                     TapOrTapMap sources) throws IOException {

    pipe = new DomainBuilderAssembly(properties.getDomainName(), pipe, keyFieldName, valueFieldName);

    // Open new version and check for success
    openNewVersion();

    Flow flow = null;
    try {

      // Build flow
      flow = getFlow(cascasdingProperties, sources);

      // Set up job
      DomainBuilderOutputCommitter.setupJob(properties.getDomainName(), flow.getJobConf());

      // Complete flow
      flow.complete();

      // Commit job
      DomainBuilderOutputCommitter.commitJob(properties.getDomainName(), flow.getJobConf());

    } catch (Exception e) {
      String exceptionMessage = "Failed at building version " + domainVersion.getVersionNumber() +
          " of domain " + properties.getDomainName() + ". Cancelling version.";
      // In case of failure, cancel this new version
      cancelNewVersion();
      // Clean up job
      if (flow != null) {
        DomainBuilderOutputCommitter.cleanupJob(properties.getDomainName(), flow.getJobConf());
      }
      e.printStackTrace();
      throw new IOException(exceptionMessage, e);
    }
    // Close the new version
    closeNewVersion();
    // Clean up job
    DomainBuilderOutputCommitter.cleanupJob(properties.getDomainName(), flow.getJobConf());
    return flow;
  }

  // Build multiple domains
  public static Flow buildDomains(Properties cascadingProperties,
                                  Map<String, Tap> sources,
                                  Map<String, Tap> otherSinks,
                                  Pipe[] otherTails,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {

    // Info output
    for (CascadingDomainBuilder domainBuilder : domainBuilders) {
      LOG.info("Building domain with " + domainBuilder.toString());
    }

    Flow flow = null;
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
        domainBuilder.properties.setCascadingProperties(cascadingProperties,
            domainBuilder.domainVersion.getVersionNumber());
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
        jobName.append(domainBuilder.domain.getName()).append(" version ")
            .append(domainBuilder.domainVersion.getVersionNumber());
      }

      // Build flow
      flow = new FlowConnector(cascadingProperties)
          .connect(jobName.toString(), sources, sinks, tails);

      // Set up jobs
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        DomainBuilderOutputCommitter.setupJob(domainBuilder.domain.getName(), flow.getJobConf());
      }

      // Complete flow
      flow.complete();

      // Commit jobs
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        DomainBuilderOutputCommitter.commitJob(domainBuilder.domain.getName(), flow.getJobConf());
      }

    } catch (Exception e) {
      // In case of failure, cancel new versions
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        domainBuilder.cancelNewVersion();
        // Clean up jobs
        if (flow != null) {
          DomainBuilderOutputCommitter.cleanupJob(domainBuilder.domain.getName(), flow.getJobConf());
        }
      }
      e.printStackTrace();
      throw new IOException("Failed at building domains. Cancelling open versions.", e);
    }
    // Close new versions
    for (CascadingDomainBuilder domainBuilder : domainBuilders) {
      domainBuilder.closeNewVersion();
      // Clean up jobs
      DomainBuilderOutputCommitter.cleanupJob(domainBuilder.domain.getName(), flow.getJobConf());
    }
    return flow;
  }

  // Build a single domain using a single source
  public Flow build(Map<Object, Object> cascadingProperties,
                    Tap source) throws IOException {
    return build(mapToProperties(cascadingProperties), source);
  }

  // Build a single domain using a multiple sources
  public Flow build(Map<Object, Object> cascadingProperties,
                    Map<String, Tap> sources) throws IOException {
    return build(mapToProperties(cascadingProperties), sources);
  }

  // Build a single domain
  public Flow build(Map<Object, Object> cascadingProperties,
                    TapOrTapMap sources) throws IOException {
    return build(mapToProperties(cascadingProperties), sources);
  }

  // Build multiple domains
  public static Flow buildDomains(Properties cascadingProperties,
                                  Map<String, Tap> sources,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {
    return buildDomains(cascadingProperties, sources, new HashMap<String, Tap>(), new Pipe[0], domainBuilders);
  }

  // Build multiple domains
  public static Flow buildDomains(Map<Object, Object> cascadingProperties,
                                  Map<String, Tap> sources,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {
    return buildDomains(mapToProperties(cascadingProperties), sources, new HashMap<String, Tap>(), new Pipe[0], domainBuilders);
  }

  // Build multiple domains
  public static Flow buildDomains(Map<Object, Object> cascadingProperties,
                                  Map<String, Tap> sources,
                                  Map<String, Tap> otherSinks,
                                  Pipe[] otherTails,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {
    return buildDomains(mapToProperties(cascadingProperties), sources, otherSinks, otherTails, domainBuilders);
  }

  public String toString() {
    return "CascadingDomainBuilder: Domain: " + properties.getDomainName() + ", Output Tap: " + outputTap;
  }

  private static Properties mapToProperties(Map<Object, Object> properties) {
    Properties newProperties = new Properties();
    newProperties.putAll(properties);
    return newProperties;
  }

  private String getFlowName() {
    return "HankCascadingDomainBuilder: " +
        properties.getDomainName() + " version " + domainVersion.getVersionNumber();
  }

  private FlowConnector getFlowConnector(Properties cascadingProperties) {
    return new FlowConnector(properties.setCascadingProperties(cascadingProperties,
        domainVersion.getVersionNumber()));
  }

  private Flow getFlow(Properties cascadingProperties,
                       TapOrTapMap sources) {
    if (sources.isTapMap()) {
      return getFlowConnector(cascadingProperties).connect(getFlowName(), sources.getTapMap(), outputTap, pipe);
    } else {
      return getFlowConnector(cascadingProperties).connect(getFlowName(), sources.getTap(), outputTap, pipe);
    }
  }
}
