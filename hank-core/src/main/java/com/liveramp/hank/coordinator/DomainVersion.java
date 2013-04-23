package com.liveramp.hank.coordinator;

import com.liveramp.hank.generated.PartitionMetadata;

import java.io.IOException;
import java.util.Collection;

public interface DomainVersion extends Comparable<DomainVersion> {

  public int getVersionNumber();

  public Long getClosedAt() throws IOException;

  public void close() throws IOException;

  public void cancel() throws IOException;

  public Collection<PartitionMetadata> getPartitionsMetadata() throws IOException;

  public void addPartitionProperties(int partNum, long numBytes, long numRecords) throws IOException;

  public boolean isDefunct() throws IOException;

  public void setDefunct(boolean isDefunct) throws IOException;

  public DomainVersionProperties getProperties() throws IOException;

  public void setProperties(DomainVersionProperties properties) throws IOException;
}
