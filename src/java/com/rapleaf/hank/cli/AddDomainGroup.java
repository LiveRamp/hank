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

import com.rapleaf.hank.util.CliUtils;
import com.rapleaf.hank.util.ZooKeeperUtils;
import com.rapleaf.hank.zookeeper.ZooKeeperConnection;

public class AddDomainGroup extends ZooKeeperConnection {
  
  public AddDomainGroup(String connectString) throws InterruptedException {
    super(connectString);
  }
  
  public void addDomainGroup(String dgName) throws InterruptedException {
    String dgPath = ZooKeeperUtils.DOMAIN_GROUP_ROOT + '/' + dgName;
    ZooKeeperUtils.createNodeRecursively(zk, dgPath + "/domains");
    ZooKeeperUtils.createNodeRecursively(zk, dgPath + "/versions");
  }

  public static void main(String args[]) throws InterruptedException {
    Options options = new Options();
    options.addOption(CliUtils.ZK_OPTION);
    options.addOption(CliUtils.buildOneArgOption("name", "the name of the domain group", "dg_name", true));
    CommandLine line = CliUtils.parseAndHelp("add_domain_group.sh", options, args);
    
    AddDomainGroup add = new AddDomainGroup(line.getOptionValue("zk"));
    add.addDomainGroup(line.getOptionValue("name"));
  }
}
