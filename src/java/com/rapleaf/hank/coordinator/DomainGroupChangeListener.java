package com.rapleaf.hank.coordinator;


/**
 * Used to receive the latest configuration information when a domain group
 * has changed. Usually this occurs when a domain has been updated to a newer
 * version, and hence its domain group is also updated to a newer version.
 */
public interface DomainGroupChangeListener {
  /**
   * Called when the configuration information for a domain group has changed.
   * The latest configuration information is supplied in the arguments.
   * 
   * @param newDomainGroup
   *          the latest configuration information for a domain group
   */
  public void onDomainGroupChange(DomainGroupConfig newDomainGroup);
}