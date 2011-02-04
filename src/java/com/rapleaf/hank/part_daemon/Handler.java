/**
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.part_daemon;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartDaemon.Iface;
import com.rapleaf.hank.storage.Result;

public class Handler implements Iface {
  private final Domain[] domains = null;

  public Handler(Configurator configurator) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public HankResponse get(byte domainId, ByteBuffer key) throws TException {
    Result result = new Result();
    Domain domain = getDomain(domainId & 0xff);

    if (domain == null) {
      return HankResponse.no_such_domain(true);
    }

    try {
      domain.get(key, result);
    } catch (IOException e) {
      // TODO: log this exception

      return HankResponse.internal_error(true);
    }
    return HankResponse.value(result.getBuffer());
  }

  private Domain getDomain(int domainId) {
    if (domains.length >= domainId) {
      return null;
    }
    return domains[domainId];
  }
}
