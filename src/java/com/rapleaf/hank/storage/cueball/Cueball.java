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
import java.io.PrintWriter;
import java.io.StringWriter;
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

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.util.FsUtils;

/**
 * Cueball is a storage engine optimized for small, fixed-size values.
 */
public class Cueball implements StorageEngine {
  private static final IFileSelector cueballFileSelector = new CueballFileSelector();

  private static final Pattern BASE_OR_DELTA_PATTERN = Pattern.compile(".*(\\d{5})\\.((base)|(delta))\\.cueball");
  static final String BASE_REGEX = ".*\\d{5}\\.base\\.cueball";
  static final String DELTA_REGEX = ".*\\d{5}\\.delta\\.cueball";

  public static class Factory implements StorageEngineFactory {
    public static final String REMOTE_DOMAIN_ROOT_KEY = "remote_domain_root";
    public static final String HASH_INDEX_BITS_KEY = "hash_index_bits";
    public static final String VALUE_SIZE_KEY = "value_size";
    public static final String KEY_HASH_SIZE_KEY = "key_hash_size";
    public static final String FILE_OPS_FACTORY_KEY = "file_ops_factory";
    public static final String HASHER_KEY = "hasher";
    public static final String COMPRESSION_CODEC = "compression_codec";

    private static final Set<String> REQUIRED_KEYS = new HashSet<String>(Arrays.asList(REMOTE_DOMAIN_ROOT_KEY, HASH_INDEX_BITS_KEY, HASHER_KEY, VALUE_SIZE_KEY, KEY_HASH_SIZE_KEY, FILE_OPS_FACTORY_KEY));

    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, String domainName) throws IOException {
      for (String requiredKey : REQUIRED_KEYS) {
        if (options == null || options.get(requiredKey) == null) {
          throw new RuntimeException("Required key '" + requiredKey
              + "' was not found!");
        }
      }

      Hasher hasher;
      IFileOpsFactory fileOpsFactory;
      try {
        hasher = (Hasher) Class.forName((String) options.get(HASHER_KEY)).newInstance();
        fileOpsFactory = (IFileOpsFactory) Class.forName((String) options.get(FILE_OPS_FACTORY_KEY)).newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      String compressionCodec = (String) options.get(COMPRESSION_CODEC);
      Class<? extends CompressionCodec> compressionCodecClass = NoCompressionCodec.class;
      if (compressionCodec != null) {
        try {
          compressionCodecClass = (Class<? extends CompressionCodec>) Class.forName(compressionCodec);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Failed to get CompressionCodec class '"
              + compressionCodec + "'!", e);
        }
      }

      return new Cueball((Integer) options.get(KEY_HASH_SIZE_KEY), hasher, (Integer) options.get(VALUE_SIZE_KEY), (Integer) options.get(HASH_INDEX_BITS_KEY), (String) options.get(REMOTE_DOMAIN_ROOT_KEY), fileOpsFactory, compressionCodecClass, domainName);
    }

    @Override
    public String getPrettyName() {
      return "Cueball";
    }

    @Override
    public String getDefaultOptions() {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);

      pw.println("---");
      pw.println("key_hash_size: 10");
      pw.println("hasher: " + Murmur64Hasher.class.getName());
      pw.println("max_allowed_part_size: " + 1024 * 1024);
      pw.println("hash_index_bits: 15");
      pw.println("remote_domain_root: #fill this in!");
      pw.println("file_ops_factory: " + LocalFileOps.Factory.class.getName());

      return sw.toString();
    }
  }

  private final String domainName;

  private final int keyHashSize;
  private final Hasher hasher;
  private final int valueSize;
  private final int hashIndexBits;
  private final String remoteDomainRoot;
  private final IFileOpsFactory fileOpsFactory;
  private final ByteBuffer keyHashBuffer;

  private final Class<? extends CompressionCodec> compressionCodecClass;

  public Cueball(int keyHashSize,
      Hasher hasher,
      int valueSize,
      int hashIndexBits,
      String remoteDomainRoot,
      IFileOpsFactory fileOpsFactory,
      Class<? extends CompressionCodec> compressionCodecClass,
      String domainName) {
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.hashIndexBits = hashIndexBits;
    this.remoteDomainRoot = remoteDomainRoot;
    this.fileOpsFactory = fileOpsFactory;
    this.keyHashBuffer = ByteBuffer.allocate(keyHashSize);
    this.compressionCodecClass = compressionCodecClass;
    this.domainName = domainName;
  }

  @Override
  public Reader getReader(PartservConfigurator configurator, int partNum) throws IOException {
    return new CueballReader(getLocalDir(configurator, partNum), keyHashSize, hasher, valueSize, hashIndexBits, getCompressionCodec());
  }

  private CompressionCodec getCompressionCodec() throws IOException {
    try {
      return compressionCodecClass.newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Writer getWriter(OutputStreamFactory outputStream, int partNum, int versionNumber, boolean base) throws IOException {
    return new CueballWriter(outputStream.getOutputStream(partNum, getName(versionNumber, base)), keyHashSize, hasher, valueSize, getCompressionCodec(), hashIndexBits);
  }

  @Override
  public Updater getUpdater(PartservConfigurator configurator, int partNum) throws IOException {
    String localDir = getLocalDir(configurator, partNum);
    return new CueballUpdater(localDir, keyHashSize, valueSize, fileOpsFactory.getFileOps(localDir, remoteDomainRoot
        + "/" + partNum), cueballFileSelector, getCompressionCodec(), hashIndexBits);
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    hasher.hash(key, keyHashBuffer.array());
    return keyHashBuffer;
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

  private String getLocalDir(PartservConfigurator configurator, int partNum) {
    ArrayList<String> l = new ArrayList<String>(configurator.getLocalDataDirectories());
    Collections.sort(l);
    return l.get(partNum % l.size()) + "/" + domainName + "/" + partNum;
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

  @Override
  public String toString() {
    return "Cueball [compressionCodecClass=" + compressionCodecClass
        + ", domainName=" + domainName + ", fileOpsFactory=" + fileOpsFactory
        + ", hashIndexBits=" + hashIndexBits + ", hasher=" + hasher
        + ", keyHashSize=" + keyHashSize + ", remoteDomainRoot="
        + remoteDomainRoot + ", valueSize=" + valueSize + "]";
  }
}
