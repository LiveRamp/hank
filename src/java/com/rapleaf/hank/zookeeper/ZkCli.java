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

package com.rapleaf.hank.zookeeper;

import com.rapleaf.hank.util.CommandLineChecker;
import org.apache.zookeeper.KeeperException;

public class ZkCli extends ZooKeeperConnection {

  public ZkCli(String connectString) throws InterruptedException {
    super(connectString);
  }

  public static void main(String[] args) throws InterruptedException, KeeperException {
    CommandLineChecker.check(args, new String[] {"connect_string", "[null|rmr]", "argument"}, ZkCli.class);
    String connectString = args[0];
    String command = args[1];
    String argument = args[2];

    ZkCli zkCli = new ZkCli(connectString);

    if (command.equals("rmr")) {
      System.out.println("Removing recursively node: " + argument);
      zkCli.zk.deleteNodeRecursively(argument);
    } else if (command.equals("null")) {
      System.out.println("Setting node to null: " + argument);
      zkCli.zk.setData(argument, null, -1);
    } else {
      System.err.println("Unknown command: " + command);
    }
  }

}
