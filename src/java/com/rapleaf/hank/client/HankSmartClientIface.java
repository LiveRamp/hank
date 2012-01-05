package com.rapleaf.hank.client;

import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient;

import java.nio.ByteBuffer;
import java.util.List;

public interface HankSmartClientIface extends SmartClient.Iface{

  @Override
  public abstract HankResponse get(String domain_name, ByteBuffer key);

  @Override
  public abstract HankBulkResponse getBulk(String domain_name, List<ByteBuffer> keys);

  public abstract void stop();

}
