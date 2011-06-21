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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MockHostDomainPartition extends AbstractHostDomainPartition {

  private final int partNum;
  private final int curVer;
  private final int nextVer;
  public int updatingToVersion;
  private Map<String, Long> counters = new HashMap<String, Long>();
  

  public MockHostDomainPartition(int partNum, int curVer, int nextVer) {
    this.partNum = partNum;
    this.curVer = curVer;
    this.nextVer = nextVer;
  }

  @Override
  public Integer getCurrentDomainGroupVersion() throws IOException {
    return curVer;
  }

  @Override
  public int getPartNum() {
    return partNum;
  }

  @Override
  public Integer getUpdatingToDomainGroupVersion() throws IOException {
    return nextVer;
  }

  @Override
  public void setCurrentDomainGroupVersion(int version) {}

  @Override
  public void setUpdatingToDomainGroupVersion(Integer version) {
    updatingToVersion = version;
  }

  @Override
  public void removeCount(String countID) throws IOException {
    if (counters.containsKey(countID)) {
      counters.remove(countID);
    }
  }

  @Override
  public void setCount(String countID, long count) throws IOException {
    counters.put(countID, count);
  }

  @Override
  public Long getCount(String countID) throws IOException {
    if (counters.containsKey(countID)) {
      return counters.get(countID);
    }
    return null;
  }

  @Override
  public Set<String> getCountKeys() throws IOException {
    return counters.keySet();
  }
}
