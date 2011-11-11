package com.rapleaf.hank.storage;

import com.rapleaf.hank.BaseTestCase;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class TestAbstractLocalFetcherUpdater extends BaseTestCase {

  private String localRoot = localTmpDir;

  private static MockFetcher mockFetcher = new MockFetcher();

  public void testCleanupAfterFailure() throws IOException {
    AbstractLocalFetcherUpdater updater = new AbstractLocalFetcherUpdater(mockFetcher, localRoot) {
      @Override
      protected void runUpdate(int toVersion) throws IOException {
        throw new Error("Fail");
      }

      @Override
      protected int getLatestLocalVersionNumber() {
        return 0;
      }
    };

    try {
      updater.update(0, Collections.<Integer>emptySet());
      fail("Update should fail");
    } catch (Throwable e) {
      // Good
    }

    // Verify that workspace directory was deleted
    String[] files = new File(localRoot).list();
    assertEquals(0, files.length);
  }

  public void testCleanupPreviousWorkdirectories() throws IOException {
    AbstractLocalFetcherUpdater updater = new AbstractLocalFetcherUpdater(mockFetcher, localRoot) {
      @Override
      protected void runUpdate(int toVersion) throws IOException {
      }

      @Override
      protected int getLatestLocalVersionNumber() {
        return 0;
      }
    };

    // Create fake previous workspace dir
    assertTrue(new File(localRoot + "/" + AbstractLocalFetcherUpdater.WORKSPACE_DIRECTORY_PREFIX + "bla/bla").mkdirs());
    assertEquals(1, new File(localRoot).list().length);

    // Update
    updater.update(0, Collections.<Integer>emptySet());

    // Verify that workspace directory was cleaned
    assertEquals(0, new File(localRoot).list().length);
  }
}
