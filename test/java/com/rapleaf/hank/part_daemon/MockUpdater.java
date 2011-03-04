package com.rapleaf.hank.part_daemon;

import java.io.IOException;

import com.rapleaf.hank.storage.Updater;

public class MockUpdater implements Updater {
  private boolean updated = false;

  @Override
  public void update(int toVersion) throws IOException {
    setUpdated(true);
  }

  public void setUpdated(boolean updated) {
    this.updated = updated;
  }

  public boolean isUpdated() {
    return updated;
  }

}
