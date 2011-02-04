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
package com.rapleaf.hank.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import com.rapleaf.hank.util.Bytes;
import com.rapleaf.hank.util.CliUtils;
import com.rapleaf.hank.util.ZooKeeperUtils;
import com.rapleaf.hank.zookeeper.ZooKeeperConnection;

public class AddDomain extends ZooKeeperConnection {
  public AddDomain(String connectString) throws InterruptedException {
    super(connectString);
  }

  public void addDomain(String domainName, String numParts, String factoryName, String factoryOptions, String partitionerName, String version)
    throws InterruptedException {
    String domainPath = ZooKeeperUtils.getDomainPath(domainName);
    ZooKeeperUtils.createNodeRecursively(zk, domainPath);
    ZooKeeperUtils.setDataOrFailSilently(zk, domainPath + "/num_parts", Bytes.stringToBytes(numParts));
    ZooKeeperUtils.setDataOrFailSilently(zk, domainPath + "/storage_engine_factory_class", Bytes.stringToBytes(factoryName));
    if (!StringUtils.isBlank(factoryOptions)) {
      ZooKeeperUtils.setDataOrFailSilently(zk, domainPath + "/storage_engine_options", Bytes.stringToBytes(factoryOptions));
    }
    ZooKeeperUtils.setDataOrFailSilently(zk, domainPath + "/partitioner", Bytes.stringToBytes(partitionerName));
    ZooKeeperUtils.setDataOrFailSilently(zk, domainPath + "/version", Bytes.stringToBytes(version));
  }

  public static void main(String[] args) throws InterruptedException {
    Options options = new Options();
    options.addOption(CliUtils.ZK_OPTION);
    options.addOption(CliUtils.buildOneArgOption("name", "the name of the domain", "domain_name", true));
    options.addOption(CliUtils.buildOneArgOption("parts", "the number of partitions for this domain", "num", true));
    options.addOption(CliUtils.buildOneArgOption("se_factory", "The storage engine factory class", "classname", true));
    options.addOption(CliUtils.buildOneArgOption("se_options", "YAML file containing options for storage engine factory", "file", false));
    options.addOption(CliUtils.buildOneArgOption("partitioner", "the partitioner that the domain uses", "classname", true));
    options.addOption(CliUtils.buildOneArgOption("version", "The current version of the domain. Defaults to 1", "num", false));
    CommandLine line = CliUtils.parseAndHelp("add_domain.sh", options, args);

    String factoryOptions = CliUtils.fileToString(line.getOptionValue("se_options"));

    AddDomain add = new AddDomain(line.getOptionValue("zk"));
    add.addDomain(line.getOptionValue("name"), line.getOptionValue("parts"), line.getOptionValue("se_factory"), 
        factoryOptions, line.getOptionValue("partitioner"), line.getOptionValue("version", "1"));
  }
}
