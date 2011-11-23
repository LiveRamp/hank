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

package com.rapleaf.hank.monitor.notification;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.monitor.notifier.Notification;
import org.apache.commons.lang.NotImplementedException;

public class HostStateNotification extends AbstractNotification implements Notification {

  private final Host host;
  private final HostState hostState;

  public HostStateNotification(final Host host, final HostState hostState) {
    this.host = host;
    this.hostState = hostState;
  }

  public Host getHost() {
    return host;
  }

  public HostState getHostState() {
    return hostState;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HostStateNotification that = (HostStateNotification) o;

    if (host != null ? !host.equals(that.host) : that.host != null) return false;
    if (hostState != that.hostState) return false;

    return true;
  }

  @Override
  public int hashCode() {
    throw new NotImplementedException();
  }

  @Override
  protected String formatCore() {
    return "Host " + host + " state is now " + hostState;
  }
}
