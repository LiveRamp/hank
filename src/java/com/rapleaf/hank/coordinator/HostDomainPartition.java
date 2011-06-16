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
import java.util.Set;

/**
 * Info about a partition within a given HostDomain.
 */
public interface HostDomainPartition extends Comparable<HostDomainPartition> {
  public int getPartNum();
  public Integer getCurrentDomainGroupVersion() throws IOException;
  public void setCurrentDomainGroupVersion(int version) throws IOException;
  public Integer getUpdatingToDomainGroupVersion() throws IOException;
  public void setUpdatingToDomainGroupVersion(Integer version) throws IOException;
  
  public void removeCount(String countID) throws IOException;
  public void setCount(String countID, long count) throws IOException;
  public Long getCount(String countID) throws IOException;
  public Set<String> getCountKeys() throws IOException;
  
}
