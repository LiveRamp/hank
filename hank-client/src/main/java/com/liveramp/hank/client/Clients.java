/**
 *  Copyright 2012 LiveRamp
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

import com.liveramp.hank.Hank;
import com.liveramp.hank.generated.ClientMetadata;
import com.liveramp.hank.util.LocalHostUtils;

import java.net.UnknownHostException;

public class Clients {

  private Clients() {
  }

  public static ClientMetadata getClientMetadata(HankClientIface client) {
    String hostName;
    try {
      hostName = LocalHostUtils.getHostName();
    } catch (UnknownHostException e) {
      hostName = "unknown";
    }
    return new ClientMetadata(
        hostName,
        System.currentTimeMillis(),
        client.getClass().getName(),
        Hank.getGitCommit());
  }
}
