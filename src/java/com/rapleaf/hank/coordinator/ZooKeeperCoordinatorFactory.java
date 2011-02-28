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

import java.util.Map;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class ZooKeeperCoordinatorFactory implements CoordinatorFactory {

  public static final String KEY_CONNECT_STRING = "connect_string";
  public static final String KEY_SESSION_TIMEOUT = "session_timeout";

  @Override
  public Coordinator getCoordinator(Map<String, Object> options) {
    throw new NotImplementedException();
//    try {
//      String connectString;
//      String sessionTimeout;
//      if ((connectString = options.get(KEY_CONNECT_STRING)) == null) {
//        throw new RuntimeException("No connectString passed to the ZooKeeperCoordinatorFactory");
//      }
//      if ((sessionTimeout = options.get(KEY_SESSION_TIMEOUT)) == null) {
//        return new ZooKeeperCoordinator(connectString);
//      }
//      else {
//        return new ZooKeeperCoordinator(connectString, Integer.parseInt(sessionTimeout));
//      }
//    }
//    catch (InterruptedException e) {
//      // If we have been interrupted, then the server is probably going down.
//      return null;
//    }
  }
}
