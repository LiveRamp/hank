/**
 *  Copyright 2013 LiveRamp
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

package com.liveramp.hank.client;

import com.liveramp.hank.coordinator.Domain;

import java.nio.ByteBuffer;

class DomainAndKey {

  private final Domain domain;
  private final ByteBuffer key;

  public DomainAndKey(Domain domain, ByteBuffer key) {
    this.domain = domain;
    this.key = key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DomainAndKey that = (DomainAndKey) o;

    if (!domain.equals(that.domain)) {
      return false;
    }
    if (!key.equals(that.key)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = domain.hashCode();
    result = 31 * result + key.hashCode();
    return result;
  }
}
