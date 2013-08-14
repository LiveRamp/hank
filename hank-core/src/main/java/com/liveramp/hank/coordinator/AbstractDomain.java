/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.coordinator;

import java.io.IOException;
import java.util.Collection;

public abstract class AbstractDomain implements Domain {

  protected DomainVersion findVersion(Collection<DomainVersion> versions,
                                      int versionNumber) throws IOException {
    for (DomainVersion v : versions) {
      if (v != null && v.getVersionNumber() == versionNumber) {
        return v;
      }
    }
    return null;
  }

  @Override
  public int compareTo(Domain other) {
    return getName().compareTo(other.getName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractDomain that = (AbstractDomain) o;

    if (getId() != that.getId()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return getId();
  }

  @Override
  public String toString() {
    return String.format("AbstractDomain [id=%d, name=%s]", getId(), getName());
  }
}
