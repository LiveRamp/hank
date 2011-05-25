package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public interface DomainVersion extends Comparable<DomainVersion> {
  public int getVersionNumber();

  /**
   * Millis since epoch corresponding to when this domain version was closed.
   * @return null if not yet closed.
   * @throws IOException 
   */
  public Long getClosedAt() throws IOException;

  /**
   * Has this domain version been closed?
   * @return
   * @throws IOException 
   */
  public boolean isClosed() throws IOException;

  /**
   * Complete this version.
   * @throws IOException 
   */
  public void close() throws IOException;

  /**
   * Cancel this version and discard any associated metadata.
   */
  public void cancel() throws IOException;

  public Set<PartitionInfo> getPartitionInfos();

  public void addPartitionInfo(int partNum, long numBytes, long numRecords);
}
