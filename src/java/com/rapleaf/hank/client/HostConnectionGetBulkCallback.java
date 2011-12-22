/*
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

import com.rapleaf.hank.generated.PartitionServer;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.async.AsyncMethodCallback;

public class HostConnectionGetBulkCallback implements AsyncMethodCallback<PartitionServer.AsyncClient.getBulk_call> {

  @Override
  public void onComplete(PartitionServer.AsyncClient.getBulk_call response) {
    throw new NotImplementedException();

  }

  @Override
  public void onError(Exception exception) {
    throw new NotImplementedException();
  }
}
