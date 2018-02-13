/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.hank.coordinator;

import com.liveramp.hank.generated.PartitionMetadata;

import java.io.IOException;

public final class DomainVersions {
  private DomainVersions() {
  }

  public static boolean isClosed(DomainVersion domainVersion) throws IOException {
    return domainVersion.getClosedAt() != null;
  }

  public static boolean isCurrentlyServed(Domain domain, DomainVersion domainVersion, Coordinator coord) throws IOException {

    for (DomainGroup domainGroup : coord.getDomainGroups()) {
      DomainAndVersion version = domainGroup.getDomainVersion(domain);

      if (version != null) {
        if (version.getVersionNumber() == domainVersion.getVersionNumber()) {
          return true;
        }
      }
    }

    return false;
  }

  public static long getTotalNumBytes(DomainVersion domainVersion) throws IOException {
    long total = 0;
    for (PartitionMetadata pm : domainVersion.getPartitionsMetadata()) {
      total += pm.get_num_bytes();
    }
    return total;
  }

  public static long getTotalNumRecords(DomainVersion domainVersion) throws IOException {
    long total = 0;
    for (PartitionMetadata pm : domainVersion.getPartitionsMetadata()) {
      total += pm.get_num_records();
    }
    return total;
  }
}
