package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.config.DomainConfig;

/**
 * Used to receive the latest configuration information when a domain has
 * changed. Usually this occurs when a domain has been updated to a newer
 * version.
 */
public interface DomainChangeListener {
  /**
   * Called when the configuration information for a domain has changed. The
   * latest configuration information is supplied in the arguments.
   * 
   * @param newDomain
   *          the latest configuration information for a domain
   */
  public void onDomainChange(DomainConfig newDomain);
}