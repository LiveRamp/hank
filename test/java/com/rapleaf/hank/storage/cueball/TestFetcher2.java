package com.rapleaf.hank.storage.cueball;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.rapleaf.hank.BaseTestCase;

public class TestFetcher2 extends BaseTestCase {
  private class MFO implements IFileOps {
    Set<String> copyToLocalCalledWith = new HashSet<String>();

    @Override
    public void copyToLocal(String fileName) {
      copyToLocalCalledWith.add(fileName);
    }

    @Override
    public List<String> listFiles() throws IOException {
      return Arrays.asList("x", "y", "z");
    }
  }

  private class MFS implements IFileSelector {
    public Set<String> isRelevantCalledWith = new HashSet<String>();
    public Set<String> selectFilesToCopyCalledWith;

    @Override
    public boolean isRelevantFile(String fileName, int f, int t) {
      assertEquals("from", 7, f);
      assertEquals("to", 10, t);
      isRelevantCalledWith.add(fileName);
      return fileName.equals("x") || fileName.equals("z");
    }

    @Override
    public List<String> selectFilesToCopy(List<String> relevantFiles) {
      selectFilesToCopyCalledWith = new HashSet<String>(relevantFiles);
      return Arrays.asList("z");
    }
  }

  public void testIt() throws Exception {
    MFO mockFileOps = new MFO();
    MFS mockFileSelector = new MFS();
    Fetcher2 f = new Fetcher2(mockFileOps, mockFileSelector);

    f.fetch(7, 10);

    assertEquals("isRelevantFile called with", new HashSet<String>(Arrays.asList("x", "y", "z")), mockFileSelector.isRelevantCalledWith);
    assertEquals("selectFilesToCopy called with", new HashSet<String>(Arrays.asList("x", "z")), mockFileSelector.selectFilesToCopyCalledWith);
    assertEquals("copyToLocal called with", new HashSet<String>(Arrays.asList("z")), mockFileOps.copyToLocalCalledWith);
  }
}
