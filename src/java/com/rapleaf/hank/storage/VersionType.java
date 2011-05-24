package com.rapleaf.hank.storage;

public enum VersionType {
  BASE,
  DELTA;

  public static VersionType fromString(String s) {
    return VersionType.valueOf(s.trim().toUpperCase());
  }
}
