package com.liveramp.hank.coordinator;

public class ServingStatus {

  protected int numPartitions;
  protected int numPartitionsServedAndUpToDate;

  public ServingStatus() {
    this(0, 0);
  }

  public ServingStatus(int numPartitions, int numPartitionsServedAndUpToDate) {
    this.numPartitions = numPartitions;
    this.numPartitionsServedAndUpToDate = numPartitionsServedAndUpToDate;
  }

  public void aggregate(ServingStatus other) {
    this.numPartitions += other.numPartitions;
    this.numPartitionsServedAndUpToDate += other.numPartitionsServedAndUpToDate;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getNumPartitionsServedAndUpToDate() {
    return numPartitionsServedAndUpToDate;
  }

  public void aggregate(int numPartitions, int numPartitionsServedAndUpToDate) {
    this.numPartitions += numPartitions;
    this.numPartitionsServedAndUpToDate += numPartitionsServedAndUpToDate;
  }

  public boolean isServedAndUpToDate() {
    return numPartitions == numPartitionsServedAndUpToDate;
  }
}
