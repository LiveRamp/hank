package com.liveramp.hank.util;

import java.io.Serializable;

import com.liveramp.commons.util.MemoryUsageEstimator;
import com.liveramp.hank.generated.HankResponse;

public class HankResponseMemoryUsageEstimator implements MemoryUsageEstimator<HankResponse>, Serializable {
  @Override
  public long estimateMemorySize(HankResponse item) {
    if (item.is_set_value()) {
      return item.get_value().length;
    } else {
      return 1;
    }
  }
}
