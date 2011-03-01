package com.rapleaf.hank.cli;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.config.YamlConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.exception.DataNotFoundException;

public class AddDomainToDomainGroup {
  /**
   * @param args
   * @throws IOException 
   * @throws ParseException 
   * @throws DataNotFoundException 
   * @throws NumberFormatException 
   */
  public static void main(String[] args) throws IOException, ParseException, NumberFormatException, DataNotFoundException {
    Options options = new Options();
    options.addOption("g", "domain-group", true,
        "the name of the domain group");
    options.addOption("d", "domain", true,
        "the name of the domain to be added to the group");
    options.addOption("i", "id", true,
        "the id for the domain in this group");
    options.addOption("c", "config", true,
        "path of a valid config file with coordinator connection information");
    try {
      CommandLine line = new GnuParser().parse(options, args);
      ClientConfigurator configurator = new YamlConfigurator(line.getOptionValue("config"));
      addDomainToDomainGroup(configurator,
          line.getOptionValue("domain-group"),
          line.getOptionValue("domain"),
          Integer.parseInt(line.getOptionValue("id")));
    } catch (ParseException e) {
      new HelpFormatter().printHelp("add_domain", options);
      throw e;
    }
  }

  private static void addDomainToDomainGroup(ClientConfigurator configurator,
      String domainGroupName,
      String domainName,
      int domainId)
  throws IOException, DataNotFoundException {
    Coordinator coord = configurator.getCoordinator();
    coord.getDomainGroupConfig(domainGroupName).addDomain(coord.getDomainConfig(domainName), domainId);
  }

}
