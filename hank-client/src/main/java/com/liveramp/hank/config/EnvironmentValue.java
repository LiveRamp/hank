package com.liveramp.hank.config;

public class EnvironmentValue {

  private final String key;
  private final String value;

  public EnvironmentValue(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }


  @Override
  public String toString() {
    return "EnvironmentValue{" +
        "key='" + key + '\'' +
        ", value='" + value + '\'' +
        '}';
  }
}
