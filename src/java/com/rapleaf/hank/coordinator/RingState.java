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
 * The various states that a Ring can take on.
 */
public enum RingState {
  /**
   * The ring is open, ready to serve data or be taken down
   */
  OPEN,
  /**
   * The ring is closing - there may be one or more hosts still serving, but
   * no new clients should make requests.
   */
  CLOSING,
  /**
   * The ring is closed - there are no partition servers still serving data.
   */
  CLOSED,
  /**
   * The ring has at least one host updating.
   */
  UPDATING,
  /**
   * The ring has completed its update.
   */
  UPDATED,
  /**
   * The partition servers are starting up, but they're not completely started yet.
   */
  OPENING;
}
