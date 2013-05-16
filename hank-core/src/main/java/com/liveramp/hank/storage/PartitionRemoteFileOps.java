/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface PartitionRemoteFileOps {

  public InputStream getInputStream(String remoteRelativePath) throws IOException;

  public OutputStream getOutputStream(String remoteRelativePath) throws IOException;

  public boolean exists(String remoteRelativePath) throws IOException;

  public void copyToLocalRoot(String remoteSourceRelativePath, String localDestinationRoot) throws IOException;

  public boolean attemptDelete(String remoteRelativePath) throws IOException;

  public String getRemoteAbsolutePath(String remoteRelativePath);
}
