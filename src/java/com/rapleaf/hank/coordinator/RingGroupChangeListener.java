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
 * Used to receive the latest configuration information when a ring group has
 * changed. Currently there are two cases when
 * <code>RingGroupChangeListener</code> might be called: 1. A host's state has
 * changed, and so the <code>RingState</code> of a ring may have changed. 2. A
 * ring has been added or removed.
 */
public interface RingGroupChangeListener {
  /**
   * Called when the configuration information for a ring group has changed.
   * The latest configuration information is supplied in the arguments.
   * 
   * @param newRingGroup
   *          the latest configuration information for a ring group
   */
  public void onRingGroupChange(RingGroup newRingGroup);
}