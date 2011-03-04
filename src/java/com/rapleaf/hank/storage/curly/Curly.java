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
package com.rapleaf.hank.storage.curly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.cueball.Cueball;
import com.rapleaf.hank.util.FsUtils;

public class Curly implements StorageEngine {
  private static final Pattern BASE_OR_REGEX_PATTERN = Pattern
      .compile(".*(\\d{5})\\.((base)|(delta))\\.curly");
  static final String BASE_REGEX = "\\d{5}\\.base\\.curly";
  static final String DELTA_REGEX = "\\d{5}\\.delta\\.curly";

  public static class Factory implements StorageEngineFactory {
    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options)
        throws IOException {
      Hasher hasher;
      try {
        hasher = (Hasher)Class.forName((String)options.get("hasher")).newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return new Curly((Integer)options.get("key_hash_size"),
          hasher,
          (Integer)options.get("max_allowed_part_size"),
          (Integer)options.get("hash_index_bits"),
          (Integer)options.get("cueball_read_buffer_bytes"),
          (Integer)options.get("record_file_read_buffer_bytes"),
          (String)options.get("remote_domain_root"));
    }
  }

  private final int offsetSize;
  private final int recordFileReadBufferBytes;

  private final Cueball cueballStorageEngine;

  public Curly(int keyHashSize, Hasher hasher, int maxAllowedPartSize,
      int hashIndexBits, int cueballReadBufferBytes,
      int recordFileReadBufferBytes, String remoteDomainRoot) {
    this.recordFileReadBufferBytes = recordFileReadBufferBytes;
    this.offsetSize = (int) (Math.ceil(Math.ceil(Math.log(maxAllowedPartSize)
        / Math.log(2)) / 8.0));
    this.cueballStorageEngine = new Cueball(keyHashSize, hasher, offsetSize,
        hashIndexBits, cueballReadBufferBytes, remoteDomainRoot);
  }

  @Override
  public Reader getReader(PartservConfigurator configurator, int partNum)
      throws IOException {
    return new CurlyReader(getLocalDir(configurator, partNum),
        recordFileReadBufferBytes, cueballStorageEngine.getReader(configurator,
            partNum));
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partNum,
      int versionNumber, boolean base) throws IOException {
    return new CurlyWriter(streamFactory.getOutputStream(partNum, getName(
        versionNumber, base)), cueballStorageEngine.getWriter(streamFactory,
        partNum, versionNumber, base), offsetSize);
  }

  private String padVersion(int versionNumber) {
    return String.format("%05d", versionNumber);
  }

  @Override
  public Updater getUpdater(PartservConfigurator configurator, int partNum) {
    throw new NotImplementedException();
    // return new CurlyUpdater();
  }

  private static String getLocalDir(PartservConfigurator configurator,
      int partNum) {
    ArrayList<String> l = new ArrayList<String>(configurator
        .getLocalDataDirectories());
    Collections.sort(l);
    return l.get(partNum % l.size());
  }

  public static int parseVersionNumber(String name) {
    Matcher matcher = BASE_OR_REGEX_PATTERN.matcher(name);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("string " + name
          + " isn't a path that parseVersionNumber can parse!");
    }

    return Integer.parseInt(matcher.group(1));
  }

  public static SortedSet<String> getBases(String localPartitionRoot) {
    return FsUtils.getMatchingPaths(localPartitionRoot, BASE_REGEX);
  }

  public static SortedSet<String> getDeltas(String localPartitionRoot) {
    return FsUtils.getMatchingPaths(localPartitionRoot, DELTA_REGEX);
  }

  private String getName(int versionNumber, boolean base) {
    String s = padVersion(versionNumber) + ".";
    if (base) {
      s += "base";
    } else {
      s += "delta";
    }
    return s + ".curly";
  }
}
