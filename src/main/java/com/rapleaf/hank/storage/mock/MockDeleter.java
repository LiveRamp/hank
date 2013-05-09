package com.rapleaf.hank.storage.mock;

import com.rapleaf.hank.storage.Deleter;

import java.io.IOException;

public class MockDeleter implements Deleter {

  private boolean hasDeleted = false;

  public MockDeleter(int partNum) {
  }

  @Override
  public void delete() throws IOException {
    hasDeleted = true;
  }

  public boolean hasDeleted() {
    return hasDeleted;
  }
}
