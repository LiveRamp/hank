package com.rapleaf.hank.ui;

import com.rapleaf.hank.coordinator.HostState;

import java.io.IOException;

public class UiUtils {

  public static String hostStateToClass(HostState state) throws IOException {
    switch (state) {
      case SERVING:
        return "host-serving";
      case UPDATING:
        return "host-updating";
      case IDLE:
        return "host-idle";
      case OFFLINE:
        return "host-offline";
      default:
        throw new RuntimeException("Unknown host state.");
    }
  }
}
