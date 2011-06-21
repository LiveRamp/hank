package com.rapleaf.hank.part_daemon;

import com.rapleaf.hank.storage.Reader;

public class ReaderWithCounters {
  
  Reader reader;
  CounterCache requests;
  CounterCache hits;
  
  public ReaderWithCounters(Reader reader) {
    this.reader = reader;
    requests = new CounterCache("Requests");
    hits = new CounterCache("Hits");
  }
  
}
