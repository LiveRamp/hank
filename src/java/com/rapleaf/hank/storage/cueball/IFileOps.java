package com.rapleaf.hank.storage.cueball;

import java.io.IOException;
import java.util.List;

public interface IFileOps {

  public List<String> listFiles() throws IOException;

  public void copyToLocal(String fileName);

}
