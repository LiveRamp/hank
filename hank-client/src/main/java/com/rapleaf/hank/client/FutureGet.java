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

package com.rapleaf.hank.client;

import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import org.apache.log4j.Logger;

import java.util.concurrent.FutureTask;

public class FutureGet extends FutureTask<Object> {

  private static final Logger LOG = Logger.getLogger(FutureGet.class);

  GetTaskRunnableIface runnable;

  public FutureGet(GetTaskRunnableIface runnable) {
    super(runnable, null);
    this.runnable = runnable;
  }

  // Wait for termination and return response
  public HankResponse getResponse() {
    try {
      this.get();
    } catch (Exception e) {
      String errMsg = "Exception while executing future GET: " + e.getMessage();
      LOG.error(errMsg, e);
      return HankResponse.xception(HankException.internal_error(errMsg));
    }
    return runnable.getResponse();
  }
}
