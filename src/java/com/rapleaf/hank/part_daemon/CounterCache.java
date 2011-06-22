package com.rapleaf.hank.part_daemon;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper class that stores: 1. AtomicLong: The value of the counter 2. String:
 * The name of the counter
 */
public class CounterCache {
  private final String name;
  private final AtomicLong count;

  public CounterCache(String name) {
    this.name = name;
    count = new AtomicLong(0);
  }

  public String getName() {
    return name;
  }

  public AtomicLong getCount() {
    return count;
  }
}
