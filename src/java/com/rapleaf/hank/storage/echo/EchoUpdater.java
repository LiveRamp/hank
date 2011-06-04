package com.rapleaf.hank.storage.echo;

import java.io.IOException;
import java.util.Set;

import com.rapleaf.hank.storage.Updater;

public class EchoUpdater implements Updater {
  @Override
  public void update(int toVersion, Set<Integer> excludeVersions) throws IOException {
    // intentionally left blank!
  }
}
