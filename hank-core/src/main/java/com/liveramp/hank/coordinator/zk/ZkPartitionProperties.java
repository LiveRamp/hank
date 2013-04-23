package com.liveramp.hank.coordinator.zk;

import org.apache.zookeeper.KeeperException;

import com.liveramp.hank.coordinator.PartitionProperties;
import com.liveramp.hank.zookeeper.ZkPath;
import com.liveramp.hank.zookeeper.ZooKeeperPlus;

public class ZkPartitionProperties implements PartitionProperties {

  private final int partNum;
  private final long numBytes;
  private final long numRecords;

  public static ZkPartitionProperties create(ZooKeeperPlus zk, String partsRoot, int partNum, long numBytes, long numRecords) throws KeeperException, InterruptedException {
    String partPath = ZkPath.append(partsRoot, nodeName(partNum));
    // if the node already exists, then don't try to create a new one
    if (zk.exists(partPath, false) == null) {
      zk.create(partPath, null);
      zk.createLong(ZkPath.append(partPath, "num_bytes"), numBytes);
      zk.createLong(ZkPath.append(partPath, "num_records"), numRecords);
      zk.create(ZkPath.append(partPath, DotComplete.NODE_NAME), null);
    }
    return new ZkPartitionProperties(zk, partPath);
  }

  public ZkPartitionProperties(ZooKeeperPlus zk, String partInfoPath)
      throws KeeperException, InterruptedException {
    String filename = ZkPath.getFilename(partInfoPath);
    String[] tokens = filename.split("-");
    this.partNum = Integer.parseInt(tokens[tokens.length - 1]);
    this.numBytes = zk.getLong(ZkPath.append(partInfoPath, "num_bytes"));
    this.numRecords = zk.getLong(ZkPath.append(partInfoPath, "num_records"));
  }

  @Override
  public long getNumBytes() {
    return numBytes;
  }

  @Override
  public long getNumRecords() {
    return numRecords;
  }

  @Override
  public int getPartitionNumber() {
    return partNum;
  }

  public static String nodeName(int partNum) {
    return String.format("part-%d", partNum);
  }
}
