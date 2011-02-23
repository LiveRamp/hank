package com.rapleaf.hank.coordinator;

import java.io.IOException;

public interface HostConfig {
  public interface HostStateChangeListener {
    public void stateChange(HostConfig hostConfig);
  }

  public PartDaemonAddress getAddress();

  public HostState getHostState() throws IOException;

  public PartDaemonState getPartDaemonState() throws IOException;
  public void setPartDaemonState(PartDaemonState state) throws IOException;

  public UpdateDaemonState getUpdateDaemonState() throws IOException;
  public void setUpdateDaemonState(UpdateDaemonState state) throws IOException;

  public void setStateChangeListener(HostStateChangeListener listener) throws IOException;
}
