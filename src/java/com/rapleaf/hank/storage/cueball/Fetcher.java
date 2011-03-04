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
package com.rapleaf.hank.storage.cueball;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class Fetcher implements IFetcher {
  private static final Logger LOG = Logger.getLogger(Fetcher.class);

  private final String remotePartitionRoot;
  private final String localPartitionRoot;

  public Fetcher(String localPartitionRoot, String remotePartitionRoot) {
    this.localPartitionRoot = localPartitionRoot;
    this.remotePartitionRoot = remotePartitionRoot;
  }

  @Override
  public void fetch(int localVersionNumber, int desiredVersion) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());

    FileStatus[] filesInRemoteRoot = fs.listStatus(new Path(remotePartitionRoot));
    List<Path> unfilteredFilesToCopy = new ArrayList<Path>();

    for (FileStatus stat : filesInRemoteRoot) {
      String name = stat.getPath().getName();
      if (isRelevantFile(name)) {
        unfilteredFilesToCopy.add(stat.getPath());
      }
    }

    int cutoffVersionNumber = getCutoffVersionNumber(unfilteredFilesToCopy, localVersionNumber);

    List<Path> newFilesToCopy = selectFilesToCopy(cutoffVersionNumber, unfilteredFilesToCopy);
    for (Path p : newFilesToCopy) {
      LOG.error("Copying file " + p + " to local...");
      copyToLocalFile(fs, p, localPartitionRoot + "/" + p.getName());
    }
  }

  protected boolean isRelevantFile(String name) {
    return name.matches(Cueball.BASE_REGEX) || name.matches(Cueball.DELTA_REGEX);
  }

  private void copyToLocalFile(FileSystem fs, Path p, String localPath) throws IOException {
    FileStatus stat = fs.getFileStatus(p);
    long total = 0;

    InputStream in = fs.open(p);
    OutputStream out = new FileOutputStream(localPath);
    byte[] buf = new byte[32*1024];
    while (total < stat.getLen()) {
      int amountRead = in.read(buf);
      out.write(buf, 0, amountRead);
      total += amountRead;
    }
    in.close();
    out.close();
  }

  private int getCutoffVersionNumber(List<Path> unfilteredFilesToCopy, int localVersionNumber) {
    Collections.sort(unfilteredFilesToCopy);
    Collections.reverse(unfilteredFilesToCopy);

    for (Path p : unfilteredFilesToCopy) {
      if (p.getName().matches(Cueball.BASE_REGEX)) {
        int versionNumber = Cueball.parseVersionNumber(p.getName());
        return versionNumber;
      }
      if (p.getName().matches(Cueball.DELTA_REGEX)) {
        int versionNumber = Cueball.parseVersionNumber(p.getName());
        if (versionNumber <= localVersionNumber) {
          break;
        }
      }
    }

    return localVersionNumber;
  }

  private List<Path> selectFilesToCopy(int cutoffVersionNumber,
      List<Path> unfilteredFilesToCopy)
  {
    List<Path> filteredFilesToCopy = new ArrayList<Path>();

    for (Path p : unfilteredFilesToCopy) {
      if (newerThanCutoff(p, cutoffVersionNumber)) {
        filteredFilesToCopy.add(p);
      }
    }
    return filteredFilesToCopy;
  }

  protected boolean newerThanCutoff(Path p, int cutoffVersionNumber) {
    String name = p.getName();
    return (name.matches(Cueball.BASE_REGEX) || name.matches(Cueball.DELTA_REGEX))
      && Cueball.parseVersionNumber(name) >= cutoffVersionNumber;
  }

}
