package com.rapleaf.hank.cascading;

import cascading.tap.Tap;

import java.util.Map;

public class TapOrTapMap {

  private final boolean isTapMapFlag;
  private final Tap tap;
  private final Map<String, Tap> tapMap;

  public TapOrTapMap(Tap tap) {
    isTapMapFlag = false;
    this.tap = tap;
    this.tapMap = null;
  }

  public TapOrTapMap(Map<String, Tap> tapMap) {
    isTapMapFlag = true;
    this.tap = null;
    this.tapMap = tapMap;
  }

  public Tap getTap() {
    if (isTapMap()) {
      throw new RuntimeException("Must be a single tap.");
    }
    return tap;
  }

  public Map<String, Tap> getTapMap() {
    if (!isTapMap()) {
      throw new RuntimeException("Must be a tap map.");
    }
    return tapMap;
  }

  public boolean isTapMap() {
    return isTapMapFlag;
  }
}
