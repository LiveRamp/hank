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

package com.rapleaf.hank.storage.incremental;

import com.rapleaf.hank.test.BaseTestCase;

import java.io.File;
import java.io.IOException;

public abstract class IncrementalPartitionUpdaterTestCase extends BaseTestCase {

  protected final String remotePartitionRoot = localTmpDir + "/remote_partition_root";
  protected final String localPartitionRoot = localTmpDir + "/partition_root";
  protected final String updateWorkRoot =
      localPartitionRoot + "/" + IncrementalPartitionUpdater.UPDATE_WORK_ROOT_PREFIX;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    new File(remotePartitionRoot).mkdir();
    new File(localPartitionRoot).mkdir();
  }

  protected void makeRemoteFile(String name) throws IOException {
    File file = new File(remotePartitionRoot + "/" + name);
    file.getParentFile().mkdirs();
    file.createNewFile();
  }

  protected void deleteRemoteFile(String name) throws IOException {
    new File(remotePartitionRoot + "/" + name).delete();
  }

  protected void makeLocalFile(String name) throws IOException {
    File file = new File(localPartitionRoot + "/" + name);
    file.getParentFile().mkdirs();
    file.createNewFile();
  }

  protected void makeLocalDir(String name) throws IOException {
    File file = new File(localPartitionRoot + "/" + name);
    file.mkdirs();
  }

  protected void deleteLocalFile(String name) throws IOException {
    new File(localPartitionRoot + "/" + name).delete();
  }

  protected void makeLocalCacheFile(String name) throws IOException {
    makeLocalFile(IncrementalPartitionUpdater.CACHE_ROOT_NAME + "/" + name);
  }

  protected void deleteLocalCacheFile(String name) throws IOException {
    deleteLocalFile(IncrementalPartitionUpdater.CACHE_ROOT_NAME + "/" + name);
  }

  protected boolean existsLocalFile(String name) {
    return new File(localPartitionRoot + "/" + name).exists();
  }

  protected boolean existsCacheFile(String name) {
    return existsLocalFile(IncrementalPartitionUpdater.CACHE_ROOT_NAME + "/" + name);
  }

  protected boolean existsUpdateWorkFile(String name) {
    return existsLocalFile(IncrementalPartitionUpdater.UPDATE_WORK_ROOT_PREFIX + "/" + name);
  }
}
