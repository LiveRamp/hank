package com.rapleaf.hank.storage.echo;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.Writer;

public class Echo implements StorageEngine {

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getReader(PartservConfigurator configurator, int partNum) throws IOException {
    return new EchoReader(partNum);
  }

  @Override
  public Updater getUpdater(PartservConfigurator configurator, int partNum) throws IOException {
    return new EchoUpdater();
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partNum, int versionNumber, boolean base) throws IOException {
    throw new UnsupportedOperationException();
  }
}
