/**
 *  Copyright 2011 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
