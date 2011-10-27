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

public final class RingGroups {
  private RingGroups() {}

  public static boolean isUpdating(RingGroup ringGroup) throws IOException {
    return ringGroup.getUpdatingToVersion() != null;
  }

  public static boolean isAssigned(RingGroup ringGroup, DomainGroupVersion domainGroupVersion) throws IOException {
    for (Ring ring : ringGroup.getRings()) {
      if (!Rings.isAssigned(ring, domainGroupVersion)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isUpToDate(RingGroup ringGroup, DomainGroupVersion domainGroupVersion) throws IOException {
    for (Ring ring : ringGroup.getRings()) {
      if (!Rings.isUpToDate(ring, domainGroupVersion)) {
        return false;
      }
    }
    return true;
  }

  public static UpdateProgress computeUpdateProgress(RingGroup ringGroup,
                                                     DomainGroupVersion domainGroupVersion) throws IOException {
    UpdateProgress result = new UpdateProgress();
    for (Ring ring : ringGroup.getRings()) {
      result.aggregate(Rings.computeUpdateProgress(ring, domainGroupVersion));
    }
    return result;
  }
}
