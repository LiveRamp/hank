package com.rapleaf.hank.storage.echo;

import com.rapleaf.hank.storage.Deleter;

public class EchoDeleter implements Deleter {
  private final int partNum;

  public EchoDeleter(int partNum) {
    this.partNum = partNum;
  }
  
  public void delete() {
  }

}
