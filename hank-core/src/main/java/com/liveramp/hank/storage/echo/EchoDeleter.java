package com.liveramp.hank.storage.echo;

import com.liveramp.hank.storage.Deleter;

public class EchoDeleter implements Deleter {
  private final int partNum;

  public EchoDeleter(int partNum) {
    this.partNum = partNum;
  }
  
  public void delete() {
  }

}
