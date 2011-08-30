package com.rapleaf.hank.coordinator;

import java.util.regex.Pattern;

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
    return action != null;
  }

  public int getVersion() {
    return version;
  }

  public Action getAction() {
    return action;
  }

  public static VersionOrAction parse(String s) {
    if (Pattern.compile("\\d*").matcher(s).matches()) {
      return new VersionOrAction(Integer.parseInt(s));
    }
    return new VersionOrAction(Action.valueOf(s));
  }

  @Override
  public String toString() {
    return "VersionOrAction [action=" + action + ", version=" + version + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((action == null) ? 0 : action.hashCode());
    result = prime * result + ((version == null) ? 0 : version.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    VersionOrAction other = (VersionOrAction) obj;
    if (action == null) {
      if (other.action != null)
        return false;
    } else if (!action.equals(other.action))
      return false;
    if (version == null) {
      if (other.version != null)
        return false;
    } else if (!version.equals(other.version))
      return false;
    return true;
  }

  public String encode() {
    if (isAction()) {
      return action.toString();
    }
    return Integer.toString(version);
  }
}
