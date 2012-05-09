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


/**
 * Used to receive the latest configuration information when a domain group
 * has changed. Usually this occurs when a domain has been updated to a newer
 * version, and hence its domain group is also updated to a newer version.
 */
public interface DomainGroupChangeListener {
  /**
   * Called when the configuration information for a domain group has changed.
   * The latest configuration information is supplied in the arguments.
   * 
   * @param newDomainGroup
   *          the latest configuration information for a domain group
   */
  public void onDomainGroupChange(DomainGroup newDomainGroup);
}
