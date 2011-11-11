/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.rapleaf.hank.storage;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public abstract class AbstractLocalFetcherUpdater implements Updater {

  private final IFetcher fetcher;
  private final String localRoot;
  private String localWorkspaceRoot;
  private File localWorkspaceRootFile;

  protected final static String WORKSPACE_DIRECTORY_PREFIX = "_tmp_"
      + AbstractLocalFetcherUpdater.class.getSimpleName() + "_";

  public AbstractLocalFetcherUpdater(IFetcher fetcher, String localRoot) {
    this.fetcher = fetcher;
    this.localRoot = localRoot;
  }

  protected String getLocalRoot() {
    return localRoot;
  }

  protected String getLocalWorkspaceRoot() {
    return localWorkspaceRoot;
  }

  public void update(int toVersion, Set<Integer> excludeVersions) throws IOException {
    // Delete previous workspace directories
    for (File file : new File(localRoot).listFiles()) {
      if (file.isDirectory() && file.getName().startsWith(WORKSPACE_DIRECTORY_PREFIX)) {
        FileUtils.deleteDirectory(file);
        if (file.exists()) {
          throw new IOException("Failed to delete workspace directory leftover from a previous update: "
              + file.getPath());
        }
      }
    }
    // Set up workspace directory
    localWorkspaceRoot = localRoot
        + "/" + WORKSPACE_DIRECTORY_PREFIX + UUID.randomUUID().toString();
    localWorkspaceRootFile = new File(localWorkspaceRoot);
    if (!localWorkspaceRootFile.mkdirs()) {
      throw new IOException("Failed to create local workspace root " + localWorkspaceRoot);
    }
    // Run update
    try {
      // Fetch
      fetcher.fetch(getLatestLocalVersionNumber(), toVersion, excludeVersions, localWorkspaceRoot);
      // Update
      runUpdate(toVersion);
      // Commit
      commitUpdate();
    } finally {
      deleteWorkspaceRoot();
    }
  }

  protected abstract void runUpdate(int toVersion) throws IOException;

  // -1 means no version available
  protected abstract int getLatestLocalVersionNumber();

  private void commitUpdate() {
    // Move all files in workspace root to root
    File[] files = new File(localWorkspaceRoot).listFiles();
    if (files != null) {
      Map<File, File> renamedFiles = new HashMap<File, File>(files.length);
      for (File file : files) {
        File renamedFile = new File(localRoot + "/" + file.getName());
        if (file.renameTo(renamedFile)) {
          renamedFiles.put(file, renamedFile);
        } else {
          // Failed to commit one file, reverting what we have commited so far
          for (Map.Entry<File, File> entry : renamedFiles.entrySet()) {
            if (!entry.getValue().renameTo(entry.getKey())) {
              throw new RuntimeException("FATAL: failed to revert unsuccessful update commit.");
            }
          }
        }
      }
    }
  }

  private void deleteWorkspaceRoot() throws IOException {
    if (localWorkspaceRootFile != null) {
      FileUtils.deleteDirectory(localWorkspaceRootFile);
    }
  }
}
