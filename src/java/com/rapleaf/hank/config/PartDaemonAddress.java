package com.rapleaf.hank.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A helper class representing a hostname and port number pair used to identify
 * a Part Daemon.
 */
public final class PartDaemonAddress {
  private static final Pattern HOST_AND_PORT_PATTERN = Pattern.compile("(\\w+):(\\d+)");
  private final String hostName;
  private final int portNumber;

  public PartDaemonAddress(String hostName, int portNumber) {
    this.hostName = hostName;
    this.portNumber = portNumber;
  }

  public String getHostName() {
    return hostName;
  }

  public int getPortNumber() {
    return portNumber;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
    result = prime * result + portNumber;
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
    PartDaemonAddress other = (PartDaemonAddress) obj;
    if (hostName == null) {
      if (other.hostName != null)
        return false;
    } else if (!hostName.equals(other.hostName))
      return false;
    if (portNumber != other.portNumber)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return hostName + ":" + portNumber;
  }

  public static PartDaemonAddress parse(String s) {
    Matcher matcher = HOST_AND_PORT_PATTERN.matcher(s);
    if (matcher.matches()) {
      return new PartDaemonAddress(matcher.group(1), Integer.parseInt(matcher.group(2)));
    } else {
      throw new RuntimeException(s + " is not a properly formatted host:port pair.");
    }
  }
}
