package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.coordinator.PartitionInfo;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.KeeperException;

public class ZkPartitionInfo implements PartitionInfo {
  private final int partNum;
  private final long numBytes;
  private final long numRecords;

  public static ZkPartitionInfo create(ZooKeeperPlus zk, String partsRoot, int partNum, long numBytes, long numRecords) throws KeeperException, InterruptedException {
    String partPath = partsRoot + "/" + nodeName(partNum);
    // if the node already exists, then don't try to create a new one
    if (zk.exists(partPath, false) == null) {
      zk.create(partPath, null);
      zk.create(partPath + "/num_bytes", numBytes);
      zk.create(partPath + "/num_records", numRecords);
      zk.create(partPath + "/.complete", null);
    }
    return new ZkPartitionInfo(zk, partPath);
  }

  public ZkPartitionInfo(ZooKeeperPlus zk, String partInfoPath)
      throws KeeperException, InterruptedException {
    String[] toks = partInfoPath.split("/");
    toks = toks[toks.length - 1].split("-");
    this.partNum = Integer.parseInt(toks[toks.length - 1]);

    this.numBytes = zk.getLong(partInfoPath + "/num_bytes");
    this.numRecords = zk.getLong(partInfoPath + "/num_records");
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
  public int getPartNum() {
    return partNum;
  }

  public static String nodeName(int partNum) {
    return String.format("part-%d", partNum);
  }
}
