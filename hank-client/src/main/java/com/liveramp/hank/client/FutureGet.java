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

package com.liveramp.hank.client;

import java.util.concurrent.FutureTask;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.hank.generated.HankException;
import com.liveramp.hank.generated.HankResponse;

public class FutureGet extends FutureTask<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(FutureGet.class);

  GetTaskRunnableIface runnable;

  public FutureGet(GetTaskRunnableIface runnable) {
    super(runnable, null);
    this.runnable = runnable;
  }

  // Wait for termination and return response
  public HankResponse getResponse() {
    try {
      this.get();
    } catch (Throwable t) {
      String errMsg = "Throwable while executing future GET: " + t.toString();
      LOG.error(errMsg, t);
      return HankResponse.xception(HankException.internal_error(errMsg));
    }
    return runnable.getResponse();
  }
}
