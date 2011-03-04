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
import com.rapleaf.hank.util.FsUtils;

public class Cueball implements StorageEngine {
  private static final Pattern BASE_OR_DELTA_PATTERN =
    Pattern.compile(".*(\\d{5})\\.((base)|(delta))\\.cueball");
  static final String BASE_REGEX = "\\d{5}\\.base\\.cueball";
  static final String DELTA_REGEX = "\\d{5}\\.delta\\.cueball";

  public static class Factory implements StorageEngineFactory {
    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options)
        throws IOException {
      throw new NotImplementedException();
    }
  }

  private final int keyHashSize;
  private final Hasher hasher;
  private final int valueSize;
  private final int hashIndexBits;
  private final int readBufferBytes;
  private final String remoteDomainRoot;

  public Cueball(int keyHashSize, Hasher hasher, int valueSize, int hashIndexBits, int readBufferBytes, String remoteDomainRoot) {
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.hashIndexBits = hashIndexBits;
    this.readBufferBytes = readBufferBytes;
    this.remoteDomainRoot = remoteDomainRoot;
  }

  @Override
  public Reader getReader(PartservConfigurator configurator, int partNum) throws IOException {
    return new CueballReader(getLocalDir(configurator, partNum), keyHashSize, hasher, valueSize, hashIndexBits, readBufferBytes);
  }

  @Override
  public Writer getWriter(OutputStreamFactory outputStream, int partNum, int versionNumber, boolean base) throws IOException {
    return new CueballWriter(outputStream.getOutputStream(partNum, getName(versionNumber, base)), keyHashSize, hasher, valueSize);
  }

  @Override
  public Updater getUpdater(PartservConfigurator configurator, int partNum) {
    return new CueballUpdater(getLocalDir(configurator, partNum), remoteDomainRoot + "/" + partNum, keyHashSize, valueSize);
  }

  static String padVersion(int ver) {
    return String.format("%05d", ver);
  }

  public static SortedSet<String> getBases(String localPartitionRoot) {
    return FsUtils.getMatchingPaths(localPartitionRoot, BASE_REGEX);
  }

  public static SortedSet<String> getDeltas(String localPartitionRoot) {
    return FsUtils.getMatchingPaths(localPartitionRoot, DELTA_REGEX);
  }

  public static int parseVersionNumber(String name) {
    Matcher matcher = BASE_OR_DELTA_PATTERN.matcher(name);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("string " + name
          + " isn't a path that parseVersionNumber can parse!");
    }

    return Integer.parseInt(matcher.group(1));
  }

  private static String getLocalDir(PartservConfigurator configurator, int partNum) {
    ArrayList<String> l = new ArrayList<String>(configurator.getLocalDataDirectories());
    Collections.sort(l);
    return l.get(partNum % l.size());
  }

  private String getName(int versionNumber, boolean base) {
    String s = padVersion(versionNumber) + ".";
    if (base) {
      s += "base";
    } else {
      s += "delta";
    }
    return s + ".cueball";
  }
}
