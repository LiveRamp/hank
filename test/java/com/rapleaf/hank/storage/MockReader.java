package com.rapleaf.hank.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.rapleaf.hank.config.PartservConfigurator;

public class MockReader implements Reader {

  private final PartservConfigurator configurator;
  private final int partNum;
  private final byte[] returnValue;

  public MockReader(PartservConfigurator configurator, int partNum, byte[] returnValue) {
    this.configurator = configurator;
    this.partNum = partNum;
    this.returnValue = returnValue;
  }

  @Override
  public void get(ByteBuffer key, Result result) throws IOException {
    result.requiresBufferSize(returnValue.length);
    result.getBuffer().position(0).limit(returnValue.length);
    result.getBuffer().put(returnValue);
    result.getBuffer().flip();
    result.found();
  }

  public PartservConfigurator getConfigurator() {
    return configurator;
  }

  public int getPartNum() {
    return partNum;
  }
}
