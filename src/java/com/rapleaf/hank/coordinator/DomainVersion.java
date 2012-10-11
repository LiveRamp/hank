package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.generated.PartitionMetadata;

import java.io.IOException;
import java.util.Collection;

public interface DomainVersion extends Comparable<DomainVersion> {

  public int getVersionNumber();

  /**
   * Millis since epoch corresponding to when this domain version was closed.
   *
   * @return null if not yet closed.
   * @throws IOException
   */
  public Long getClosedAt() throws IOException;

  /**
   * Complete this version.
   *
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   * Cancel this version and discard any associated metadata.
   */
  public void cancel() throws IOException;

  public Collection<PartitionMetadata> getPartitionsMetadata() throws IOException;

  public void addPartitionProperties(int partNum, long numBytes, long numRecords) throws IOException;

  /**
   * A defunct version should not be used or deployed.
   *
   * @return
   * @throws IOException
   */
  public boolean isDefunct() throws IOException;

  public void setDefunct(boolean isDefunct) throws IOException;

  public DomainVersionProperties getProperties() throws IOException;

  public void setProperties(DomainVersionProperties properties) throws IOException;
}
