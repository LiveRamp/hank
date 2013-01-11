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

package com.rapleaf.hank.storage.cueball;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.hank.storage.incremental.IncrementalRemoteDomainCleaner;
import com.rapleaf.hank.storage.RemoteDomainCleaner;

import java.io.IOException;

public class CueballRemoteDomainCleaner extends IncrementalRemoteDomainCleaner implements RemoteDomainCleaner {

  public CueballRemoteDomainCleaner(Domain domain,
                                    int numRemoteLeafVersionsToKeep) {
    super(domain, numRemoteLeafVersionsToKeep);
  }

  protected DomainVersion getParentDomainVersion(Domain domain, DomainVersion domainVersion) throws IOException {
    return IncrementalDomainVersionProperties.getParentDomainVersion(domain, domainVersion);
  }
}
