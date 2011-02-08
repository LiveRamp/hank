package com.rapleaf.hank.update_daemon;

import java.io.IOException;

import com.rapleaf.hank.storage.Updater;

public class MockUpdater implements Updater {
  private boolean updated = false;

  @Override
  public void update() throws IOException {
    setUpdated(true);
  }

  public void setUpdated(boolean updated) {
    this.updated = updated;
  }

  public boolean isUpdated() {
    return updated;
  }

}
