package com.rapleaf.hank.coordinator;

import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class DomainGroups {

  private static final Logger LOG = Logger.getLogger(DomainGroups.class);

  private DomainGroups() {
  }


  public static void fastForwardDomain(DomainGroup group, Domain domain) throws IOException {
    DomainVersion version = Domains.getLatestVersionNotOpenNotDefunct(domain);
    group.setDomainVersion(domain, version.getVersionNumber());
  }

  public static void fastForwardDomains(DomainGroup group, Set<Domain> domains) throws IOException {
    Map<Domain, Integer> domainVersions = Maps.newHashMap();
    for (Domain domain : domains) {
      domainVersions.put(domain, Domains.getLatestVersionNotOpenNotDefunct(domain).getVersionNumber());
    }
    group.mergeDomainVersions(domainVersions);
  }

  public static void fastForwardAllDomains(DomainGroup group) throws IOException {
    fastForwardDomains(group, group.getDomains());
  }

}
