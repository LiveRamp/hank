package com.liveramp.hank.ring_group_conductor;

import java.util.HashMap;
import java.util.Map;

import com.liveramp.commons.collections.CountingMap;

public class HostReplicaStatus {

  private final int minRingFullyServingObservations;
  private final int minServingReplicas;
  private final int minServingAvailabilityBucketReplicas;
  private final double minServingFraction;
  private final double minServingAvailabilityBucketFraction;
  private final String availabilityBucketKey;
  private final CountingMap<String> hostToFullyServingObservations = new CountingMap<String>();

  public Long getObservations(String host){
    return hostToFullyServingObservations.get().get(host);
  }

  public Long incrementObservations(String host){
    hostToFullyServingObservations.increment(host, 1l);
    return hostToFullyServingObservations.get().get(host);
  }

  public void clearObservations(String host){
    hostToFullyServingObservations.get().put(host, 0L);
  }

  public HostReplicaStatus(
      int minRingFullyServingObservations, int minServingReplicas, int minServingAvailabilityBucketReplicas, double minServingFraction, double minServingAvailabilityBucketFraction, String availabilityBucketKey) {
    this.minRingFullyServingObservations = minRingFullyServingObservations;
    this.minServingReplicas = minServingReplicas;
    this.minServingAvailabilityBucketReplicas = minServingAvailabilityBucketReplicas;
    this.minServingFraction = minServingFraction;
    this.minServingAvailabilityBucketFraction = minServingAvailabilityBucketFraction;
    this.availabilityBucketKey = availabilityBucketKey;
  }

  public int getMinRingFullyServingObservations() {
    return minRingFullyServingObservations;
  }

  public int getMinServingReplicas() {
    return minServingReplicas;
  }

  public int getMinServingAvailabilityBucketReplicas() {
    return minServingAvailabilityBucketReplicas;
  }

  public double getMinServingFraction() {
    return minServingFraction;
  }

  public double getMinServingAvailabilityBucketFraction() {
    return minServingAvailabilityBucketFraction;
  }

  public String getAvailabilityBucketKey() {
    return availabilityBucketKey;
  }
}
