package com.liveramp.hank.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public final class CommandLineChecker {

  public static final void check(CommandLine commandLine, Options options, String[] requiredOptions, Class clazz) {
    for (String option : requiredOptions) {
      if (!commandLine.hasOption(option)) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(clazz.getSimpleName(), options);
        System.exit(1);
      }
    }
  }

  public static final void check(String[] arguments, String[] expectedArguments, Class clazz) {
    if (arguments.length != expectedArguments.length) {
      StringBuilder usage = new StringBuilder();
      usage.append(clazz.getSimpleName());
      for (String arg : expectedArguments) {
        usage.append(" <");
        usage.append(arg);
        usage.append(">");
      }
      System.err.println(usage);
      System.exit(1);
    }
  }

}
