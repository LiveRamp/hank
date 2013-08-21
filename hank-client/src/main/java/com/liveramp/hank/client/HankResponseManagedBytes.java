/**
 *  Copyright 2013 LiveRamp
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

package com.liveramp.hank.client;

import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.util.ManagedBytes;

public class HankResponseManagedBytes implements ManagedBytes {

  private final HankResponse response;

  public HankResponseManagedBytes(HankResponse response) {
    this.response = response;
  }

  public HankResponse getResponse() {
    return response;
  }

  @Override
  public long getNumManagedBytes() {
    if (response.is_set_value()) {
      return response.get_value().length;
    } else {
      return 1;
    }
  }
}
