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
package com.rapleaf.hank;

import java.io.File;

import junit.framework.TestCase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.rapleaf.hank.util.FsUtils;

public abstract class BaseTestCase extends TestCase {
  static {
    Logger.getRootLogger().setLevel(Level.ALL);
  }

  protected final String localTmpDir = System.getProperty("tmpDir", "/tmp/hank_local_file_system");

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    FsUtils.rmrf(localTmpDir);
    new File(localTmpDir).mkdirs();
    System.err.println("------ test start ------");
    System.out.println("------ test start ------");
  }
}
