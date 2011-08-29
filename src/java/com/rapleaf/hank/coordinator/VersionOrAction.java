package com.rapleaf.hank.coordinator;

public class VersionOrAction {
  public enum Action {
    UNASSIGN
  }

  private final Integer version;
  private final Action action;

  public VersionOrAction(int version) {
    this.version = version;
    this.action = null;
  }

  public VersionOrAction(Action action) {
    this.version = null;
    this.action = action;
  }

  public boolean isAction() {
    return action == null;
  }

  public int getVersion() {
    return version;
  }

  public Action getAction() {
    return action;
  }
}
