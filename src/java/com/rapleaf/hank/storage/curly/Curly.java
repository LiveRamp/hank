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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.cueball.Cueball;
import com.rapleaf.hank.storage.cueball.IFileOpsFactory;
import com.rapleaf.hank.util.FsUtils;

/**
 * Curly is a storage engine designed for larger, variable-sized values. It uses
 * Cueball under the hood.
 */
public class Curly implements StorageEngine {
  private static final Pattern BASE_OR_REGEX_PATTERN = Pattern
  .compile(".*(\\d{5})\\.((base)|(delta))\\.curly");
  static final String BASE_REGEX = ".*\\d{5}\\.base\\.curly";
  static final String DELTA_REGEX = ".*\\d{5}\\.delta\\.curly";

  public static class Factory implements StorageEngineFactory {
    public static final String REMOTE_DOMAIN_ROOT_KEY = "remote_domain_root";
    public static final String RECORD_FILE_READ_BUFFER_BYTES_KEY = "record_file_read_buffer_bytes";
    public static final String CUEBALL_READ_BUFFER_BYTES_KEY = "cueball_read_buffer_bytes";
    public static final String HASH_INDEX_BITS_KEY = "hash_index_bits";
    public static final String MAX_ALLOWED_PART_SIZE_KEY = "max_allowed_part_size";
    public static final String KEY_HASH_SIZE_KEY = "key_hash_size";
    public static final String FILE_OPS_FACTORY_KEY = "file_ops_factory";
    public static final String HASHER_KEY = "hasher";

    private static final Set<String> REQUIRED_KEYS = new HashSet<String>(Arrays.asList(
        REMOTE_DOMAIN_ROOT_KEY,
        RECORD_FILE_READ_BUFFER_BYTES_KEY,
        CUEBALL_READ_BUFFER_BYTES_KEY,
        HASH_INDEX_BITS_KEY,
        MAX_ALLOWED_PART_SIZE_KEY,
        KEY_HASH_SIZE_KEY,
        FILE_OPS_FACTORY_KEY,
        HASHER_KEY
    ));

    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, String domainName)
    throws IOException {
      for (String requiredKey : REQUIRED_KEYS) {
        if (options == null || options.get(requiredKey) == null) {
          throw new RuntimeException("Required key '" + requiredKey + "' was not found!");
        }
      }

      Hasher hasher;
      IFileOpsFactory fileOpsFactory;
      try {
        hasher = (Hasher)Class.forName((String)options.get(HASHER_KEY)).newInstance();
        fileOpsFactory = (IFileOpsFactory)Class.forName((String)options.get(FILE_OPS_FACTORY_KEY)).newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return new Curly((Integer)options.get(KEY_HASH_SIZE_KEY),
          hasher,
          (Integer)options.get(MAX_ALLOWED_PART_SIZE_KEY),
          (Integer)options.get(HASH_INDEX_BITS_KEY),
          (Integer)options.get(CUEBALL_READ_BUFFER_BYTES_KEY),
          (Integer)options.get(RECORD_FILE_READ_BUFFER_BYTES_KEY),
          (String)options.get(REMOTE_DOMAIN_ROOT_KEY),
          fileOpsFactory,
          domainName);
    }
  }

  private final String domainName;

  private final int offsetSize;
  private final int recordFileReadBufferBytes;

  private final Cueball cueballStorageEngine;
  private final String remoteDomainRoot;
  private final int keyHashSize;
  private final int cueballReadBufferBytes;
  private final IFileOpsFactory fileOpsFactory;

  public Curly(int keyHashSize,
      Hasher hasher,
      int maxAllowedPartSize,
      int hashIndexBits,
      int cueballReadBufferBytes,
      int recordFileReadBufferBytes,
      String remoteDomainRoot,
      IFileOpsFactory fileOpsFactory,
      String domainName)
  {
    this.keyHashSize = keyHashSize;
    this.cueballReadBufferBytes = cueballReadBufferBytes;
    this.recordFileReadBufferBytes = recordFileReadBufferBytes;
    this.remoteDomainRoot = remoteDomainRoot;
    this.fileOpsFactory = fileOpsFactory;
    this.domainName = domainName;
    this.offsetSize = (int) (Math.ceil(Math.ceil(Math.log(maxAllowedPartSize)
        / Math.log(2)) / 8.0));
    this.cueballStorageEngine = new Cueball(keyHashSize,
        hasher,
        offsetSize,
        hashIndexBits,
        cueballReadBufferBytes,
        remoteDomainRoot,
        fileOpsFactory,
        domainName);
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
      int versionNumber, boolean base)
  throws IOException {
    OutputStream outputStream = streamFactory.getOutputStream(partNum, getName(versionNumber, base));
    Writer cueballWriter = cueballStorageEngine.getWriter(streamFactory, partNum, versionNumber, base);
    return new CurlyWriter(outputStream, cueballWriter, offsetSize);
  }

  private String padVersion(int versionNumber) {
    return String.format("%05d", versionNumber);
  }

  @Override
  public Updater getUpdater(PartservConfigurator configurator, int partNum) {
    String localDir = getLocalDir(configurator, partNum);
    new File(localDir).mkdirs();
    String remotePartRoot = remoteDomainRoot + "/" + partNum;
    return new CurlyUpdater(localDir,
        remotePartRoot,
        keyHashSize,
        offsetSize,
        cueballReadBufferBytes,
        fileOpsFactory.getFileOps(localDir, remotePartRoot));
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    return cueballStorageEngine.getComparableKey(key);
  }

  private String getLocalDir(PartservConfigurator configurator,
      int partNum) {
    ArrayList<String> l = new ArrayList<String>(configurator
        .getLocalDataDirectories());
    Collections.sort(l);
    return l.get(partNum % l.size()) + "/" + domainName + "/" + partNum;
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

  @Override
  public String toString() {
    return "Curly [domain=" + domainName + ", cueballReadBufferBytes=" + cueballReadBufferBytes
    + ", " + cueballStorageEngine + ", keyHashSize="
    + keyHashSize + ", offsetSize=" + offsetSize
    + ", recordFileReadBufferBytes=" + recordFileReadBufferBytes
    + ", remoteDomainRoot=" + remoteDomainRoot + "]";
  }
}
