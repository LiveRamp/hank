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

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.YamlClientConfigurator;

public class AddDomainGroup {
  public static void addDomainGroup(ClientConfigurator configurator, String name) throws InterruptedException, IOException {
    configurator.getCoordinator().addDomainGroup(name);
  }

  public static void main(String args[]) throws InterruptedException, IOException, ParseException, InvalidConfigurationException {
    Options options = new Options();
    options.addOption("n", "name", true,
        "the name of the domain to be created");
    options.addOption("c", "config", true,
        "path of a valid config file with coordinator connection information");
    try {
      CommandLine line = new GnuParser().parse(options, args);
      ClientConfigurator configurator = new YamlClientConfigurator(line.getOptionValue("config"));
      addDomainGroup(configurator, line.getOptionValue("name"));
    } catch (ParseException e) {
      new HelpFormatter().printHelp("add_domain", options);
      throw e;
    }
  }
}
