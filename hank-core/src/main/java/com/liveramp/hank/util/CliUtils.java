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
package com.liveramp.hank.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.lang.StringUtils;

public class CliUtils {
  public static final Option ZK_OPTION = buildOneArgOption("zk", "comma delimited string of host:ports for the ZooKeeper service", "ensemble", true, "quorum-connect-string");
  
  private static final Option HELP_OPTION = new Option("help", "print usage");
  
  @SuppressWarnings("static-access")
  public static Option buildOneArgOption(String name, String description, String argName, boolean required, String longOpt) {
    return OptionBuilder.withArgName(argName)
                        .hasArg()
                        .isRequired(required)
                        .withDescription(description)
                        .withLongOpt(longOpt)
                        .create(name);
  }

  public static CommandLine parseAndHelp(String appName, Options options, String[] args) {
    options.addOption(HELP_OPTION);

    Parser parser = new GnuParser();
    CommandLine line = null;
    try {
      line = parser.parse(options, args);
    } catch (MissingOptionException e) {
      new HelpFormatter().printHelp(appName, options, true);
      throw new IllegalArgumentException();
    } catch (MissingArgumentException e) {
      new HelpFormatter().printHelp(appName, options, true);
      throw new IllegalArgumentException();
    } catch (ParseException e) {
      System.err.println("Unexpected Exception: " + e);
      throw new IllegalArgumentException();
    }

    if (line.hasOption("help")) {
      new HelpFormatter().printHelp(appName, options, true);
      throw new IllegalArgumentException();
    }

    return line;
  }

  public static String fileToString(String fileName) {
    if (fileName == null || StringUtils.isBlank(fileName)) {
      return "";
    }

    File file = new File(fileName);
    String content = "";
    try {
      content = FsUtils.readFileToString(file);
    } catch (IOException e) {
      System.err.println(e.toString());
      System.exit(1);
    }
    return content;
  }
}
