package com.rapleaf.hank.coordinator;

import java.util.Set;

public interface DomainVersion extends Comparable<DomainVersion> {
  public int getVersionNumber();

  /**
   * Millis since epoch corresponding to when this domain version was closed.
   * @return null if not yet closed.
   */
  public Long getClosedAt();

  /**
   * Has this domain version been closed?
   * @return
   */
  public boolean isClosed();

  /**
   * Complete this version.
   */
  public void close();

  /**
   * Cancel this version and discard any associated metadata.
   */
  public void cancel();

  public Set<PartitionInfo> getPartitionInfos();

  public void addPartitionInfo(int partNum, long numBytes, long numRecords);
}
