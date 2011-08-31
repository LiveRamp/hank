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

/**
 * Represents a given domain in a domain group's version, capturing the domain
 * version.
 */
public interface DomainGroupVersionDomainVersion extends Comparable<DomainGroupVersionDomainVersion> {
  public Domain getDomain();

  /**
   * The domain version that this domain is within the domain group version. 
   */
  public VersionOrAction getVersionOrAction();

  public void delete() throws IOException;
}
