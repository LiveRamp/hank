package com.liveramp.hank.ring_group_conductor;

enum LiveReplicaStatus {
  UNDER_REPLICATED,
  REPLICATED,
  OVER_REPLICATED;

  public boolean isFullyReplicated() {
    return this == REPLICATED || this == OVER_REPLICATED;
  }

}
