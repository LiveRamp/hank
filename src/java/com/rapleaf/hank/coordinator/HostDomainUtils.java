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

package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public final class HostDomainUtils {

  private HostDomainUtils() {}

  public static Long getAggregateCount(HostDomain hostDomain, String countID) throws IOException {
    long aggregateCount = 0;
    boolean notNull = false;
    for (HostDomainPartition hdp : hostDomain.getPartitions()) {
      Long currentCount = hdp.getCount(countID);
      if (currentCount != null) {
        notNull = true;
        aggregateCount += currentCount;
      }
    }
    if (notNull) {
      return aggregateCount;
    }
    return null;
  }

  public static Set<String> getAggregateCountKeys(HostDomain hostDomain) throws IOException {
    Set<String> aggregateCountKeys = new HashSet<String>();
    for (HostDomainPartition hdp : hostDomain.getPartitions()) {
      aggregateCountKeys.addAll(hdp.getCountKeys());
    }
    return aggregateCountKeys;
  }
}
