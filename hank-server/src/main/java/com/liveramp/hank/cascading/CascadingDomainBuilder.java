/**
 * Copyright 2011 LiveRamp
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.hank.cascading;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowStepListener;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.liveramp.hank.coordinator.DomainVersionProperties;
import com.liveramp.hank.hadoop.DomainBuilderOutputCommitter;
import com.liveramp.hank.hadoop.DomainBuilderProperties;
import com.liveramp.hank.hadoop.DomainVersionNumberAndNumPartitions;

public class CascadingDomainBuilder {

  private final static Logger LOG = LoggerFactory.getLogger(CascadingDomainBuilder.class);

  private Tap outputTap = null;
  private final DomainBuilderProperties properties;
  private final DomainVersionProperties domainVersionProperties;
  private Pipe pipe;
  private final String keyFieldName;
  private final String valueFieldName;
  private Integer partitionToBuild = null;
  private Integer domainVersionNumber = null;
  private Integer numPartitions = null;

  public CascadingDomainBuilder(DomainBuilderProperties properties,
                                DomainVersionProperties domainVersionProperties,
                                Pipe pipe,
                                String keyFieldName,
                                String valueFieldName) throws IOException {
    this.properties = properties;
    this.domainVersionProperties = domainVersionProperties;
    this.pipe = pipe;
    this.keyFieldName = keyFieldName;
    this.valueFieldName = valueFieldName;
  }

  public void openNewVersion() throws IOException {
    DomainVersionNumberAndNumPartitions domainVersionNumberAndNumPartitions = properties.openVersion(domainVersionProperties);
    domainVersionNumber = domainVersionNumberAndNumPartitions.getDomainVersionNumber();
    numPartitions = domainVersionNumberAndNumPartitions.getNumPartitions();
    // Create Tap
    outputTap = new DomainBuilderTap(keyFieldName, valueFieldName, domainVersionNumber, properties);
  }

  public void cancelNewVersion() throws IOException {
    properties.cancelVersion(domainVersionNumber);
  }

  public void closeNewVersion() throws IOException {
    properties.closeVersion(domainVersionNumber);
  }

  public void setPartitionToBuild(int partitionToBuild) {
    this.partitionToBuild = partitionToBuild;
  }

  // Build a single domain using one source
  public Flow build(FlowStepListener listener,
                    Properties cascadingProperties,
                    String sourcePipeName,
                    Tap source) throws IOException {
    return build(listener, cascadingProperties, Cascades.tapsMap(sourcePipeName, source));
  }

  public Flow build(FlowStepListener listener,
                    Properties cascadingProperties,
                    Map<String, Tap> sources) throws IOException {
    return build(listener, new HadoopFlowConnectorFactory(cascadingProperties), sources);
  }

  // Build a single domain
  public Flow build(FlowStepListener listener,
                    FlowConnectorFactory flowConnectorFactory,
                    Map<String, Tap> sources) throws IOException {

    pipe = new DomainBuilderAssembly(properties.getDomainName(),
        pipe,
        keyFieldName,
        valueFieldName,
        properties.shouldPartitionAndSortInput(),
        partitionToBuild);

    // Open new version and check for success
    openNewVersion();

    Flow<JobConf> flow = null;
    try {

      // Build flow
      flow = getFlow(flowConnectorFactory, sources);

      // Set up job
      DomainBuilderOutputCommitter.setupJob(properties.getDomainName(), flow.getConfig());

      // Attach listener callback to get updates about job progress
      flow.addStepListener(listener);

      // Complete flow
      flow.complete();

      // Commit job
      DomainBuilderOutputCommitter.commitJob(properties.getDomainName(), flow.getConfig());

    } catch (Exception e) {
      String exceptionMessage = "Failed at building version " + domainVersionNumber +
          " of domain " + properties.getDomainName() + ". Cancelling version.";
      // In case of failure, cancel this new version
      cancelNewVersion();
      // Clean up job
      if (flow != null) {
        DomainBuilderOutputCommitter.cleanupJob(properties.getDomainName(), flow.getConfig());
      }
      e.printStackTrace();
      throw new IOException(exceptionMessage, e);
    }
    // Close the new version
    if(properties.shouldCloseVersion()) {
      closeNewVersion();
    }
    // Clean up job
    DomainBuilderOutputCommitter.cleanupJob(properties.getDomainName(), flow.getConfig());
    return flow;
  }

  public static Flow buildDomains(Properties cascadingProperties,
                                  Map<String, Tap> sources,
                                  Map<String, Tap> otherSinks,
                                  Pipe[] otherTails,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {
    return buildDomains(new HadoopFlowConnectorFactory(cascadingProperties), cascadingProperties, sources, otherSinks, otherTails, domainBuilders);
  }

  // Build multiple domains
  public static Flow buildDomains(FlowConnectorFactory flowConnectorFactory,
                                  Properties cascadingProperties,
                                  Map<String, Tap> sources,
                                  Map<String, Tap> otherSinks,
                                  Pipe[] otherTails,
                                  CascadingDomainBuilder... domainBuilders) throws IOException {
    // Info output
    for (CascadingDomainBuilder domainBuilder : domainBuilders) {
      LOG.info("Building domain with " + domainBuilder.toString());
    }

    Flow<JobConf> flow = null;
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
            domainBuilder.valueFieldName,
            domainBuilder.properties.shouldPartitionAndSortInput(),
            domainBuilder.partitionToBuild);
      }

      // Update properties
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        domainBuilder.properties.setCascadingProperties(cascadingProperties,
            domainBuilder.domainVersionNumber,
            domainBuilder.numPartitions);
      }

      // Add partition marker sources
      Map<String, Tap> actualSources = new HashMap<String, Tap>(sources);
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        actualSources.put(DomainBuilderAssembly.getPartitionMarkersPipeName(domainBuilder.properties.getDomainName()),
            new PartitionMarkerTap(domainBuilder.properties.getDomainName(),
                domainBuilder.keyFieldName,
                domainBuilder.valueFieldName));
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
        sinks.put(DomainBuilderAssembly.getSinkName(domainBuilder.properties.getDomainName()),
            domainBuilder.outputTap);
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
        jobName.append(domainBuilder.properties.getDomainName()).append(" version ")
            .append(domainBuilder.domainVersionNumber);
      }

      // Build flow
      flow = flowConnectorFactory.create(cascadingProperties).connect(jobName.toString(), actualSources, sinks, tails);

      // Set up jobs
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        DomainBuilderOutputCommitter.setupJob(domainBuilder.properties.getDomainName(), flow.getConfig());
      }

      // Complete flow
      flow.complete();

      // Commit jobs
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        DomainBuilderOutputCommitter.commitJob(domainBuilder.properties.getDomainName(), flow.getConfig());
      }

    } catch (Exception e) {
      // In case of failure, cancel new versions
      for (CascadingDomainBuilder domainBuilder : domainBuilders) {
        domainBuilder.cancelNewVersion();
        // Clean up jobs
        if (flow != null) {
          DomainBuilderOutputCommitter.cleanupJob(domainBuilder.properties.getDomainName(), flow.getConfig());
        }
      }
      e.printStackTrace();
      throw new IOException("Failed at building domains. Cancelling open versions.", e);
    }
    // Close new versions
    for (CascadingDomainBuilder domainBuilder : domainBuilders) {
      if(domainBuilder.properties.shouldCloseVersion()) {
        domainBuilder.closeNewVersion();
      }
      // Clean up jobs
      DomainBuilderOutputCommitter.cleanupJob(domainBuilder.properties.getDomainName(), flow.getConfig());
    }
    return flow;
  }

  public Integer getDomainVersionNumber() {
    return domainVersionNumber;
  }

  private DomainVersionProperties getDomainVersionProperties() {
    return domainVersionProperties;
  }

  // Build a single domain using a single source
  public Flow build(FlowStepListener listener,
                    Map<Object, Object> cascadingProperties,
                    String sourcePipeName,
                    Tap source) throws IOException {
    return build(listener, mapToProperties(cascadingProperties), sourcePipeName, source);
  }

  // Build a single domain using a multiple sources
  public Flow build(FlowStepListener listener,
                    Map<Object, Object> cascadingProperties,
                    Map<String, Tap> sources) throws IOException {
    return build(listener, mapToProperties(cascadingProperties), sources);
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

  public Properties getProperties() {
    return properties.setCascadingProperties(new Properties(),
        domainVersionNumber, numPartitions);
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
        properties.getDomainName() + " version " + domainVersionNumber;
  }

  private Flow<JobConf> getFlow(FlowConnectorFactory flowConnectorFactory,
                                Map<String, Tap> sources) {
    Map<String, Tap> actualSources = new HashMap<String, Tap>(sources);
    actualSources.put(
        DomainBuilderAssembly.getPartitionMarkersPipeName(properties.getDomainName()),
        new PartitionMarkerTap(properties.getDomainName(), keyFieldName, valueFieldName));
    return flowConnectorFactory.create(getProperties()).connect(getFlowName(), actualSources, outputTap, pipe);
  }
}
