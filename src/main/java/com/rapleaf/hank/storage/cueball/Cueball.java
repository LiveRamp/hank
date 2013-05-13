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

import com.rapleaf.hank.compression.cueball.CompressionCodec;
import com.rapleaf.hank.compression.cueball.NoCompressionCodec;
import com.rapleaf.hank.config.DataDirectoriesConfigurator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.DomainVersionPropertiesSerialization;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.hasher.IdentityHasher;
import com.rapleaf.hank.storage.*;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.hank.storage.incremental.IncrementalStorageEngine;
import com.rapleaf.hank.storage.incremental.IncrementalUpdatePlanner;
import com.rapleaf.hank.util.FsUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cueball is a storage engine optimized for small, fixed-size values.
 */
public class Cueball extends IncrementalStorageEngine implements StorageEngine {

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
    public static final String NUM_REMOTE_LEAF_VERSIONS_TO_KEEP = "num_remote_leaf_versions_to_keep";
    public static final String PARTITION_CACHE_CAPACITY = "partition_cache_capacity";

    private static final Set<String> REQUIRED_KEYS =
        new HashSet<String>(Arrays.asList(REMOTE_DOMAIN_ROOT_KEY,
            HASH_INDEX_BITS_KEY,
            HASHER_KEY,
            VALUE_SIZE_KEY,
            KEY_HASH_SIZE_KEY,
            FILE_OPS_FACTORY_KEY,
            NUM_REMOTE_LEAF_VERSIONS_TO_KEEP));

    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, Domain domain) throws IOException {
      for (String requiredKey : REQUIRED_KEYS) {
        if (options == null || options.get(requiredKey) == null) {
          throw new IOException("Required key '" + requiredKey
              + "' was not found!");
        }
      }

      // Hasher
      Hasher hasher;
      PartitionRemoteFileOpsFactory fileOpsFactory;
      try {
        hasher = (Hasher) Class.forName((String) options.get(HASHER_KEY)).newInstance();
        fileOpsFactory = (PartitionRemoteFileOpsFactory) Class.forName((String) options.get(FILE_OPS_FACTORY_KEY)).newInstance();
      } catch (Exception e) {
        throw new IOException(e);
      }

      // Compression codec
      String compressionCodec = (String) options.get(COMPRESSION_CODEC);
      Class<? extends CompressionCodec> compressionCodecClass = NoCompressionCodec.class;
      if (compressionCodec != null) {
        try {
          compressionCodecClass = (Class<? extends CompressionCodec>) Class.forName(compressionCodec);
        } catch (ClassNotFoundException e) {
          throw new IOException("Failed to get CompressionCodec class '"
              + compressionCodec + "'!", e);
        }
      }

      // Num remote bases to keep
      Integer numRemoteLeafVersionsToKeep = (Integer) options.get(NUM_REMOTE_LEAF_VERSIONS_TO_KEEP);

      // Cache capacity
      Integer partitionCacheCapacity = (Integer) options.get(PARTITION_CACHE_CAPACITY);
      if (partitionCacheCapacity == null) {
        partitionCacheCapacity = -1;
      }

      return new Cueball((Integer) options.get(KEY_HASH_SIZE_KEY),
          hasher,
          (Integer) options.get(VALUE_SIZE_KEY),
          (Integer) options.get(HASH_INDEX_BITS_KEY),
          (String) options.get(REMOTE_DOMAIN_ROOT_KEY),
          fileOpsFactory,
          compressionCodecClass,
          domain,
          numRemoteLeafVersionsToKeep,
          partitionCacheCapacity);
    }

    @Override
    public String getPrettyName() {
      return "Cueball";
    }

    @Override
    public String getDefaultOptions() {
      return "";
    }
  }

  private final Domain domain;

  private final int keyHashSize;
  private final Hasher hasher;
  private final int valueSize;
  private final int hashIndexBits;
  private final String remoteDomainRoot;
  private final PartitionRemoteFileOpsFactory partitionRemoteFileOpsFactory;
  private final ByteBuffer keyHashBuffer;
  private final int numRemoteLeafVersionsToKeep;
  private final int partitionCacheCapacity;

  private final Class<? extends CompressionCodec> compressionCodecClass;

  public Cueball(int keyHashSize,
                 Hasher hasher,
                 int valueSize,
                 int hashIndexBits,
                 String remoteDomainRoot,
                 PartitionRemoteFileOpsFactory partitionRemoteFileOpsFactory,
                 Class<? extends CompressionCodec> compressionCodecClass,
                 Domain domain,
                 int numRemoteLeafVersionsToKeep,
                 int partitionCacheCapacity) {
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.hashIndexBits = hashIndexBits;
    this.remoteDomainRoot = remoteDomainRoot;
    this.partitionRemoteFileOpsFactory = partitionRemoteFileOpsFactory;
    this.keyHashBuffer = ByteBuffer.allocate(keyHashSize);
    this.compressionCodecClass = compressionCodecClass;
    this.domain = domain;
    this.numRemoteLeafVersionsToKeep = numRemoteLeafVersionsToKeep;
    this.partitionCacheCapacity = partitionCacheCapacity;
    // Sanity check
    if (hashIndexBits > 32) {
      throw new RuntimeException("hashIndexBits is much too large (" + hashIndexBits + ")");
    }
  }

  @Override
  public Reader getReader(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException {
    return new CueballReader(getLocalDir(configurator, partitionNumber),
        keyHashSize, hasher, valueSize, hashIndexBits, getCompressionCodec(), partitionCacheCapacity);
  }

  private CompressionCodec getCompressionCodec() throws IOException {
    try {
      return compressionCodecClass.newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Writer getWriter(DomainVersion domainVersion,
                          PartitionRemoteFileOps partitionRemoteFileOps,
                          int partitionNumber) throws IOException {
    IncrementalDomainVersionProperties domainVersionProperties = getDomainVersionProperties(domainVersion);
    return new CueballWriter(partitionRemoteFileOps.getOutputStream(getName(domainVersion.getVersionNumber(),
        domainVersionProperties.isBase())),
        keyHashSize, hasher, valueSize, getCompressionCodec(), hashIndexBits);
  }

  private IncrementalDomainVersionProperties getDomainVersionProperties(DomainVersion domainVersion) throws IOException {
    IncrementalDomainVersionProperties result;
    try {
      result = (IncrementalDomainVersionProperties) domainVersion.getProperties();
    } catch (ClassCastException e) {
      throw new IOException("Failed to load properties of version " + domainVersion);
    }
    if (result == null) {
      throw new IOException("Null properties for version " + domainVersion);
    }
    return result;
  }

  @Override
  public IncrementalUpdatePlanner getUpdatePlanner(Domain domain) {
    return new CueballUpdatePlanner(domain);
  }

  @Override
  public PartitionUpdater getUpdater(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException {
    String localDir = getLocalDir(configurator, partitionNumber);
    return new CueballPartitionUpdater(domain,
        getPartitionRemoteFileOps(partitionNumber),
        new CueballMerger(),
        keyHashSize,
        valueSize,
        hashIndexBits,
        getCompressionCodec(),
        localDir);
  }

  @Override
  public Compactor getCompactor(DataDirectoriesConfigurator configurator,
                                int partitionNumber) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Writer getCompactorWriter(DomainVersion domainVersion,
                                   PartitionRemoteFileOps partitionRemoteFileOps,
                                   int partitionNumber) throws IOException {
    IncrementalDomainVersionProperties domainVersionProperties = getDomainVersionProperties(domainVersion);
    // Note: We use the identity hasher since keys coming in are already hashed keys
    return new CueballWriter(partitionRemoteFileOps.getOutputStream(getName(domainVersion.getVersionNumber(),
        domainVersionProperties.isBase())),
        keyHashSize,
        new IdentityHasher(),
        valueSize,
        getCompressionCodec(),
        hashIndexBits);
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    hasher.hash(key, keyHashSize, keyHashBuffer.array());
    return keyHashBuffer;
  }

  @Override
  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory() {
    return partitionRemoteFileOpsFactory;
  }

  @Override
  public PartitionRemoteFileOps getPartitionRemoteFileOps(int partitionNumber) throws IOException {
    return partitionRemoteFileOpsFactory.getPartitionRemoteFileOps(remoteDomainRoot, partitionNumber);
  }

  @Override
  public Deleter getDeleter(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException {
    String localDir = getLocalDir(configurator, partitionNumber);
    return new CueballDeleter(localDir);
  }

  public static String padVersionNumber(int versionNumber) {
    return String.format("%05d", versionNumber);
  }

  public static SortedSet<CueballFilePath> getBases(String... dirs) throws IOException {
    SortedSet<CueballFilePath> result = new TreeSet<CueballFilePath>();
    Set<String> paths = FsUtils.getMatchingPaths(BASE_REGEX, dirs);
    for (String path : paths) {
      result.add(new CueballFilePath(path));
    }
    return result;
  }

  public static SortedSet<CueballFilePath> getDeltas(String... dirs) throws IOException {
    SortedSet<CueballFilePath> result = new TreeSet<CueballFilePath>();
    Set<String> paths = FsUtils.getMatchingPaths(DELTA_REGEX, dirs);
    for (String path : paths) {
      result.add(new CueballFilePath(path));
    }
    return result;
  }

  public static int parseVersionNumber(String name) {
    Matcher matcher = BASE_OR_DELTA_PATTERN.matcher(name);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("string " + name
          + " isn't a path that parseVersionNumber can parse!");
    }

    return Integer.parseInt(matcher.group(1));
  }

  private String getLocalDir(DataDirectoriesConfigurator configurator, int partNum) {
    ArrayList<String> l = new ArrayList<String>(configurator.getDataDirectories());
    Collections.sort(l);
    return l.get(partNum % l.size()) + "/" + domain.getName() + "/" + partNum;
  }

  public static String getName(int versionNumber, boolean base) {
    String s = padVersionNumber(versionNumber) + ".";
    if (base) {
      s += "base";
    } else {
      s += "delta";
    }
    return s + ".cueball";
  }

  public static String getName(DomainVersion domainVersion) throws IOException {
    return getName(domainVersion.getVersionNumber(), IncrementalDomainVersionProperties.isBase(domainVersion));
  }

  @Override
  public String toString() {
    return "Cueball [compressionCodecClass=" + compressionCodecClass
        + ", domainName=" + domain.getName()
        + ", fileOpsFactory=" + partitionRemoteFileOpsFactory
        + ", hashIndexBits=" + hashIndexBits
        + ", hasher=" + hasher
        + ", keyHashSize=" + keyHashSize
        + ", remoteDomainRoot=" + remoteDomainRoot
        + ", valueSize=" + valueSize
        + ", numRemoteLeafVersionsToKeep=" + numRemoteLeafVersionsToKeep
        + "]";
  }

  @Override
  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter() throws IOException {
    return new CueballRemoteDomainVersionDeleter(domain, remoteDomainRoot, partitionRemoteFileOpsFactory);
  }

  @Override
  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException {
    return new CueballRemoteDomainCleaner(domain, numRemoteLeafVersionsToKeep);
  }

  @Override
  public DomainVersionPropertiesSerialization getDomainVersionPropertiesSerialization() {
    return new IncrementalDomainVersionProperties.Serialization();
  }
}
