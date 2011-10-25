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
package com.rapleaf.hank.storage.cueball;

import com.rapleaf.hank.BaseTestCase;

import java.io.IOException;
import java.util.*;

public class TestFetcher extends BaseTestCase {
  private class MFO implements IFileOps {
    Set<String> copyToLocalCalledWith = new HashSet<String>();

    @Override
    public String copyToLocal(String remoteFileName, String localDirectory) {
      copyToLocalCalledWith.add(remoteFileName);
      return null;
    }

    @Override
    public List<String> listFiles() throws IOException {
      return Arrays.asList("x", "y", "z");
    }

    @Override
    public boolean attemptDeleteRemote(String path) {
      return false;
    }
  }

  private class MFS implements IFileSelector {
    public Set<String> isRelevantCalledWith = new HashSet<String>();
    public Set<String> selectFilesToCopyCalledWith;

    @Override
    public boolean isRelevantFile(String fileName, Integer f, int t, Set<Integer> excludeVersions) {
      assertEquals("from", 7, f.intValue());
      assertEquals("to", 10, t);
      assertEquals(Collections.singleton(9), excludeVersions);
      isRelevantCalledWith.add(fileName);
      return fileName.equals("x") || fileName.equals("z");
    }

    @Override
    public List<String> selectFilesToCopy(List<String> relevantFiles, Integer fromVersion, int toVersion, Set<Integer> excludeVersions) {
      selectFilesToCopyCalledWith = new HashSet<String>(relevantFiles);
      return Arrays.asList("z");
    }
  }

  public void testIt() throws Exception {
    MFO mockFileOps = new MFO();
    MFS mockFileSelector = new MFS();
    Fetcher f = new Fetcher(mockFileOps, mockFileSelector);

    f.fetch(7, 10, Collections.singleton(9), null);

    assertEquals("isRelevantFile called with", new HashSet<String>(Arrays.asList("x", "y", "z")), mockFileSelector.isRelevantCalledWith);
    assertEquals("selectFilesToCopy called with", new HashSet<String>(Arrays.asList("x", "z")), mockFileSelector.selectFilesToCopyCalledWith);
    assertEquals("copyToLocal called with", new HashSet<String>(Arrays.asList("z")), mockFileOps.copyToLocalCalledWith);
  }
}
