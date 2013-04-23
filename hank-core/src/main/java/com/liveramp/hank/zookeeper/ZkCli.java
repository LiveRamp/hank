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

package com.liveramp.hank.zookeeper;

import com.liveramp.hank.util.CommandLineChecker;
import org.apache.zookeeper.KeeperException;

import java.util.List;

public class ZkCli extends ZooKeeperConnection {

  public ZkCli(String connectString) throws InterruptedException {
    super(connectString);
  }

  private int countDescendants(String node) throws InterruptedException, KeeperException {
    List<String> children = zk.getChildren(node, false);
    int count = 0;
    for (String child : children) {
      count += countDescendants(ZkPath.append(node, child));
    }
    return 1 + count;
  }

  public static void main(String[] args) throws InterruptedException, KeeperException {
    CommandLineChecker.check(args, new String[]{"connect_string", "[set|null|rmr|count|ls]", "arguments..."}, ZkCli.class);
    String connectString = args[0];
    String command = args[1];
    String argument = args[2];

    ZkCli zkCli = new ZkCli(connectString);

    if (command.equals("set")) {
      String value = args[3];
      System.out.println("Removing node value to: " + value + ", node: " + argument);
      zkCli.zk.setString(argument, value);
    } else if (command.equals("rmr")) {
      System.out.println("Removing recursively node: " + argument);
      zkCli.zk.deleteNodeRecursively(argument);
    } else if (command.equals("null")) {
      System.out.println("Setting node to null: " + argument);
      zkCli.zk.setData(argument, null, -1);
    } else if (command.equals("count")) {
      System.out.println("Counting the number of descendants in: " + argument);
      int count = zkCli.countDescendants(argument);
      System.out.println("Result: " + count);
    } else if (command.equals("ls")) {
      List<String> children = zkCli.zk.getChildren(argument, false);
      for (String child : children) {
        System.out.println(ZkPath.append(argument, child));
      }
    } else {
      System.err.println("Unknown command: " + command);
    }
  }
}
