package com.rapleaf.hank.coordinator;

import java.io.IOException;

public abstract class AbstractRingGroup implements RingGroup {
  @Override
  public boolean isUpdating() throws IOException {
    return getUpdatingToVersion() != null;
  }
}
