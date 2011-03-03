package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public interface HostConfig {
  public interface HostStateChangeListener {
    public void onHostStateChange(HostConfig hostConfig);
  }

  public PartDaemonAddress getAddress();

  public HostState getState() throws IOException;

  public void setState(HostState state) throws IOException;

  /**
   * Returns true when the host is online. Note that this is distinct from
   * "serving data" - a host is online when it's NOT offline.
   * 
   * @return
   * @throws IOException
   */
  public boolean isOnline() throws IOException;

  /**
   * Tell the host what it should do next.
   * @param command
   * @throws IOException
   */
  public void setCommand(HostCommand command) throws IOException;

  /**
   * Get the currently pending command.
   * @return
   * @throws IOException
   */
  public HostCommand getCommand() throws IOException;

  /**
   * The listener will be notified when either the state or the command changes.
   * @param listener
   * @throws IOException
   */
  public void setStateChangeListener(HostStateChangeListener listener) throws IOException;

  public Set<HostDomainConfig> getAssignedDomains() throws IOException;

  public HostDomainConfig addDomain(int domainId) throws IOException;

  public HostDomainConfig getDomainById(int domainId);
}
