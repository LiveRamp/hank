package com.rapleaf.hank.coordinator;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class DomainGroups {

  private DomainGroups() {
  }


  public static void fastForwardDomain(DomainGroup domainGroup, Domain domain) throws IOException {
    DomainVersion version = Domains.getLatestVersionNotOpenNotDefunct(domain);
    domainGroup.setDomainVersion(domain, version.getVersionNumber());
  }

  public static void fastForwardDomains(DomainGroup domainGroup, Collection<Domain> domains) throws IOException {
    Map<Domain, Integer> domainVersions = Maps.newHashMap();
    for (Domain domain : domains) {
      domainVersions.put(domain, Domains.getLatestVersionNotOpenNotDefunct(domain).getVersionNumber());
    }
    domainGroup.mergeDomainVersions(domainVersions);
  }

  public static void fastForwardAllDomains(DomainGroup domainGroup) throws IOException {
    fastForwardDomains(domainGroup, domainGroup.getDomains());
  }

}
