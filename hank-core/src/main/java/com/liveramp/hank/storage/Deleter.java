package com.liveramp.hank.storage;

import java.io.IOException;

/**
 * Interface through which individual partitions are deleted.
 */
public interface Deleter {
  public void delete() throws IOException;
}
